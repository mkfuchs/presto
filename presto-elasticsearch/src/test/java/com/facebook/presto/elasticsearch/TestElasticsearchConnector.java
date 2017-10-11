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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestElasticsearchConnector
{
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final Date DATE = new Date();
    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;

    @BeforeClass
    public void setup()
            throws Exception
    {
//        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
//        String keyspace = "test_connector";
//        initializeTestData(DATE, keyspace);

        URL metadataUrl = Resources.getResource(TestElasticsearchClient.class, "/example-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadataUri = metadataUrl.toURI();

        String connectorId = "elasticsearch-test";
        ElasticsearchConnectorFactory connectorFactory = new ElasticsearchConnectorFactory(
                ImmutableMap.of(),
                Thread.currentThread().getContextClassLoader());

        Connector connector = connectorFactory.create(
                connectorId,
                ImmutableMap.of("elasticsearch.metadata-uri", metadataUri.toString()),
                new TestingConnectorContext());

        metadata = connector.getMetadata(ElasticsearchTransactionHandle.INSTANCE);
        assertInstanceOf(metadata, ElasticsearchMetadata.class);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, ElasticsearchSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, ElasticsearchRecordSetProvider.class);

        database = ElasticsearchTestConstants.ES_SCHEMA1;
        table = new SchemaTableName(database, ElasticsearchTestConstants.ES_TBL_1);
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
    }

    @Test
    public void testGetClient()
    {
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = metadata.listSchemaNames(SESSION);
        assertTrue(databases.contains(database.toLowerCase(ENGLISH)));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(SESSION, database);
        assertTrue(tables.contains(table));
    }

    // disabled until metadata manager is updated to handle invalid catalogs and schemas
    @Test(enabled = false, expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
            throws Exception
    {
        metadata.listTables(SESSION, INVALID_DATABASE);
    }

    @Test
    public void testListUnknownSchema()
    {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("totally_invalid_database_name", "dual")));
        assertEquals(metadata.listTables(SESSION, "totally_invalid_database_name"), ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix("totally_invalid_database_name", "dual")), ImmutableMap.of());
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorTransactionHandle transaction = ElasticsearchTransactionHandle.INSTANCE;

        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(SESSION, tableHandle, Constraint.alwaysTrue(), Optional.empty());
        ConnectorTableLayoutHandle layout = getOnlyElement(layouts).getTableLayout().getHandle();
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transaction, SESSION, layout));

        long rowNumber = 0;
        for (ConnectorSplit split : splits) {
            ElasticsearchSplit elasticsearchSplit = (ElasticsearchSplit) split;

            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(transaction, SESSION, elasticsearchSplit, columnHandles).cursor()) {
                assertTrue(cursor.advanceNextPosition(), "The cursor must return at least one result");
                do {
                    try {
                        assertReadFields(cursor, tableMetadata.getColumns());
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    UUID docId = UUID.fromString(cursor.getSlice(columnIndex.get("_id")).toStringUtf8());
                    assertNotNull(docId);

                    String docIndex = cursor.getSlice(columnIndex.get("_index")).toStringUtf8();
                    assertNotNull(docIndex);
                    assertTrue(docIndex.contains("route"));

//                    int rowId = Integer.parseInt(keyValue.substring(4));
//
//                    assertEquals(keyValue, String.format("key %d", rowId));

//                    assertEquals(Bytes.toHexString(cursor.getSlice(columnIndex.get("typebytes")).getBytes()), String.format("0x%08X", rowId));

                    // VARINT is returned as a string
//                    assertEquals(cursor.getSlice(columnIndex.get("typeinteger")).toStringUtf8(), String.valueOf(rowId));
//
//                    assertEquals(cursor.getLong(columnIndex.get("typelong")), 1000 + rowId);
//
//                    assertEquals(cursor.getSlice(columnIndex.get("typeuuid")).toStringUtf8(), String.format("00000000-0000-0000-0000-%012d", rowId));
//
//                    assertEquals(cursor.getSlice(columnIndex.get("typetimestamp")).toStringUtf8(), Long.valueOf(DATE.getTime()).toString());

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                }while(cursor.advanceNextPosition() && rowNumber < 25);
            }
        }
//        assertEquals(rowNumber, 9);
    }

    private static void assertReadFields(RecordCursor cursor, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            if (!cursor.isNull(columnIndex)) {
                Type type = column.getType();
                if (BOOLEAN.equals(type)) {
                    cursor.getBoolean(columnIndex);
                }
                else if (INTEGER.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (BIGINT.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (TIMESTAMP.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (DOUBLE.equals(type)) {
                    cursor.getDouble(columnIndex);
                }
                else if (isVarcharType(type) || VARBINARY.equals(type)) {
                    try {
                        cursor.getSlice(columnIndex);
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("column " + column, e);
                    }
                }
                else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(1000)));
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            String name = Types.checkType(columnHandle, ElasticsearchColumnHandle.class, "columnHandle").getColumnName();
            index.put(name, i);
            i++;
        }
        return index.build();
    }
}
