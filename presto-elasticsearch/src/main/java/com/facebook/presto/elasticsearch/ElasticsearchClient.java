/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.inject.Inject;

import java.io.IOException;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ElasticsearchClient
{
    private static final Logger log = Logger.get(ElasticsearchClient.class);
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private Supplier<Map<String, Map<String, ElasticsearchTable>>> schemas;
    private ElasticsearchConfig config;
    private JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec;
    private ImmutableMap<String, Client> internalClients;

    @Inject
    public ElasticsearchClient(ElasticsearchConfig config, JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
            throws IOException
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        this.config = config;
        this.catalogCodec = catalogCodec;
        this.schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));

        this.internalClients = createClients(this.schemas.get());
        updateSchemas();
    }

    public ImmutableMap<String, Client> getInternalClients()
    {
        return internalClients;
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames2(String schema)
    {
        requireNonNull(schema, "schema is null");

        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);

        Client client = getInternalClients().values().iterator().next();
        GetIndexRequest indexRequest = new GetIndexRequest().indices("*");

        Set<String> indicesSet = new HashSet<>();

        try {
            String[] indices = client
                    .admin()
                    .indices()
                    .getIndex(indexRequest)
                    .get()
                    .getIndices();
            indicesSet.addAll(new HashSet<String>(Arrays.asList(indices)));
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }

        if (tables == null) {
            return ImmutableSet.of();
        }
        //return tables.keySet();
        return indicesSet;
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        try {
            updateSchemas();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);

        Client client = getInternalClients().values().iterator().next();
        GetIndexRequest indexRequest = new GetIndexRequest().indices("*");
        GetMappingsRequest mappingsRequestAll = new GetMappingsRequest().indices("*");

        Set<String> indicesSet = new HashSet<>();

        try {
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings =
                    client.admin()
                            .indices()
                            .getMappings(mappingsRequestAll)
                            .get()
                            .getMappings();

            mappings.forEach(k -> k.value.forEach(v -> indicesSet.add(k.key.replace(".", "_") + "_" + v.key)));

            //indicesSet.addAll(new HashSet<String>(Arrays.asList(indices)));
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }

        if (tables == null) {
            return ImmutableSet.of();
        }
        //return tables.keySet();
        return indicesSet;
    }

    public ElasticsearchTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema.toLowerCase(ENGLISH));
        if (tables == null) {
            return null;
        }
        return tables.get(tableName.toLowerCase(ENGLISH));
    }

    Map<String, Map<String, ElasticsearchTable>> updateSchemas()
            throws IOException
    {
        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));

        GetMappingsRequest mappingsRequestAll = new GetMappingsRequest().indices("*");

        Map<String, Map<String, ElasticsearchTable>> schemasMap = schemas.get();
        Map<String, Map<String, ElasticsearchTable>> schemasMapAll = new HashMap<>();
        for (Map.Entry<String, Map<String, ElasticsearchTable>> schemaEntry : schemasMap.entrySet()) {
            Map<String, ElasticsearchTable> tablesMap = schemaEntry.getValue();
            Map<String, ElasticsearchTable> tablesMapAll = new HashMap<>();
            for (Map.Entry<String, ElasticsearchTable> tableEntry : tablesMap.entrySet()) {
                ElasticsearchTableSource tableSource = tableEntry.getValue().getSources().iterator().next();
                Client client = internalClients.get(tableSource.getClusterName());
                //getTableNames(schemaEntry.getKey());
                try {
                    ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = client
                            .admin()
                            .indices()
                            .getMappings(mappingsRequestAll)
                            .get()
                            .getMappings();

                    mappings.forEach(k -> k.value.forEach(v ->
                    {
                        String type = v.key.toLowerCase(ENGLISH);
                        String table = k.key.replace(".", "_");
                        ElasticsearchTableSource ets = new ElasticsearchTableSource(tableSource.getHostAddress(), tableSource.getPort(), tableSource.getClusterName(), k.key, v.key);
                        ElasticsearchTable et = new ElasticsearchTable(table + "_" + type, Arrays.asList(ets));
                        tablesMapAll.put(table + "_" + type, et);
                        updateTableColumns(et);
                    }));
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            schemasMapAll.put(schemaEntry.getKey(), tablesMapAll);
        }
        schemas = Suppliers.memoize(Suppliers.ofInstance(schemasMapAll));

        return schemasMapAll;
    }

    ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getMappings(ElasticsearchTableSource src)
            throws ExecutionException, InterruptedException
    {
        int port = src.getPort();
        String hostAddress = src.getHostAddress();
        String clusterName = src.getClusterName();
        String index = src.getIndex();
        String type = src.getType();

        log.debug(String.format("Connecting to cluster %s from %s:%d, index %s, type %s", clusterName, hostAddress, port, index, type));
        Client client = internalClients.get(clusterName);
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().types(type);
        GetMappingsRequest mappingsRequestAll = new GetMappingsRequest().indices("*");

        // an index is optional - if no index is configured for the table, it will retrieve all indices for the doc type
        if (index != null && !index.isEmpty()) {
            mappingsRequest.indices(index);
        }

        return client
                .admin()
                .indices()
                .getMappings(mappingsRequest)
                .get()
                .getMappings();
    }

    Set<ElasticsearchColumn> getColumns(ElasticsearchTableSource src)
            throws ExecutionException, InterruptedException, IOException, JSONException
    {
        Set<ElasticsearchColumn> result = new HashSet();
        String type = src.getType();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings = getMappings(src);

        // what makes sense is to get the reunion of all the columns from all the mappings for the specified document type
        for (ObjectCursor<String> currentIndex : allMappings.keys()) {
            MappingMetaData mappingMetaData = allMappings.get(currentIndex.value).get(type);
            JSONObject json = new JSONObject(mappingMetaData.source().toString())
                    .getJSONObject(type)
                    .getJSONObject("properties");

            List<String> allColumnMetadata = getColumnsMetadata(null, json);
            for (String columnMetadata : allColumnMetadata) {
                ElasticsearchColumn clm = createColumn(columnMetadata);
                if (!(clm == null) && !result.contains(clm)) {
                    result.add(clm);
                }
            }
        }

        result.add(createColumn("_id.type:string"));
        result.add(createColumn("_index.type:string"));

        return result;
    }

    List<String> getColumnsMetadata(String parent, JSONObject json)
            throws JSONException
    {
        List<String> leaves = new ArrayList();

        Iterator it = json.keys();
        while (it.hasNext()) {
            String key = (String) it.next();
            Object child = json.get(key);
            String childKey = parent == null || parent.isEmpty() ? key : parent.concat(".").concat(key);

            if (child instanceof JSONObject) {
                leaves.addAll(getColumnsMetadata(childKey, (JSONObject) child));
            }
            else if (child instanceof JSONArray) {
                // ignoring arrays for now
                continue;
            }
            else {
                leaves.add(childKey.concat(":").concat(child.toString()));
            }
        }

        return leaves;
    }

    ElasticsearchColumn createColumn(String fieldPathType)
            throws JSONException, IOException
    {
        String[] items = fieldPathType.split(":");
        String path = items[0];
        Type prestoType;

        if (items.length != 2) {
            log.error("Invalid column path format. Ignoring...");
            return null;
        }

        if (!path.endsWith(".type")) {
            log.debug("Invalid column has no type info. Ignoring...");
            return null;
        }

        if (path.endsWith("raw.type")) {
            log.debug("Invalid column has no type info. Ignoring...");
            return null;
        }

        // when it is a complex type it should be handle as a nested
        String type = path.contains(".properties.") ? "nested" : items[1];

        switch (type) {
            case "double":
            case "float":
                prestoType = DOUBLE;
                break;
            case "integer":
                prestoType = INTEGER;
                break;
            case "long":
                prestoType = BIGINT;
                break;
            case "string":
                prestoType = VARCHAR;
                break;
            case "boolean":
                prestoType = BOOLEAN;
                break;
            case "binary":
                prestoType = VARBINARY;
                break;
            case "nested":
                prestoType = VARCHAR; //JSON
                break;
            default:
                prestoType = VARCHAR; //JSON
                break;
        }

        String propertyName = path.substring(0, path.indexOf('.'));

        return new ElasticsearchColumn(propertyName, prestoType, propertyName, type);
    }

    void updateTableColumns(ElasticsearchTable table)
    {
        Set<ElasticsearchColumn> columns = new HashSet();

        // the table can have multiple sources
        // the column set should be the reunion of all
        for (ElasticsearchTableSource src : table.getSources()) {
            try {
                columns.addAll(getColumns(src));
            }
            catch (ExecutionException | InterruptedException | IOException | JSONException e) {
                e.printStackTrace();
            }
        }

        table.setColumns(columns
                .stream()
                .collect(Collectors.toList()));
        table.setColumnsMetadata(columns
                .stream()
                .map(ElasticsearchColumnMetadata::new)
                .collect(Collectors.toList()));
    }

    static Map<String, Map<String, ElasticsearchTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
            throws IOException
    {
        URL url = metadataUri.toURL();
        log.debug("url: " + url);

        String tableMappings = Resources.toString(url, UTF_8);
        log.debug("tableMappings: " + tableMappings);

        Map<String, List<ElasticsearchTable>> catalog = catalogCodec.fromJson(tableMappings);
        //call code from update schemas to return list of
        return ImmutableMap.copyOf(
                transformValues(
                        catalog,
                        resolveAndIndexTablesFunction()));
    }

    static Supplier<Map<String, Map<String, ElasticsearchTable>>> schemasSupplier(final JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec, final URI metadataUri)
    {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    static Function<List<ElasticsearchTable>, Map<String, ElasticsearchTable>> resolveAndIndexTablesFunction()
    {
        return tables -> ImmutableMap.copyOf(
                uniqueIndex(
                        transform(
                                tables,
                                table -> new ElasticsearchTable(table.getName(), table.getSources())),
                        ElasticsearchTable::getName));
    }

    static ImmutableMap<String, Client> createClients(Map<String, Map<String, ElasticsearchTable>> schemas)
    {
        Map<String, Client> transportClients = new HashMap<String, Client>();

        for (String key : schemas.keySet()) {
            Map<String, ElasticsearchTable> tableMap = schemas.get(key);
            for (String tableName : tableMap.keySet()) {
                ElasticsearchTable elasticsearchTable = tableMap.get(tableName);
                List<ElasticsearchTableSource> tableSources = elasticsearchTable.getSources();
                for (ElasticsearchTableSource tableSource : tableSources) {
                    String clusterName = tableSource.getClusterName();
                    if (transportClients.get(clusterName) == null) {
                        Settings settings = ImmutableSettings.settingsBuilder()
                                .put("cluster.name", clusterName)
                                .build();
                        Client transportClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(tableSource.getHostAddress(), tableSource.getPort()));
                        transportClients.put(clusterName, transportClient);
                    }
                }
            }
        }
        return ImmutableMap.copyOf(transportClients);
    }
}
