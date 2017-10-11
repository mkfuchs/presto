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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.io.Resources;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.CONNECTOR_ID;
import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_SCHEMA1;
import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_TBL_1;
import static com.facebook.presto.elasticsearch.MetadataUtil.CATALOG_CODEC;
import static org.testng.Assert.assertNotNull;

public class TestElasticsearchRecordCursor
{
    private ElasticsearchClient client;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestElasticsearchClient.class, "/example-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadata = metadataUrl.toURI();

        client = new ElasticsearchClient(new ElasticsearchConfig().setMetadata(metadata), CATALOG_CODEC);
    }

    @Test
    public void testCursor()
    {
        ElasticsearchTable table = client.getTable(ES_SCHEMA1, ES_TBL_1);
        assertNotNull(table);

        ElasticsearchSplit split = new ElasticsearchSplit(CONNECTOR_ID, ES_SCHEMA1, ES_TBL_1, table.getSources().get(0), TupleDomain.none());

        ElasticsearchRecordCursor elasticsearchRecordCursor =
                new ElasticsearchRecordCursor(
                        getElasticsearchColumnHandles(table),
                        split, client);

        assertNotNull(elasticsearchRecordCursor);
    }

    private List<ElasticsearchColumnHandle> getElasticsearchColumnHandles(ElasticsearchTable table)
    {
        List<ElasticsearchColumnHandle> columnHandles = new ArrayList();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            ElasticsearchColumnMetadata esColumn = (ElasticsearchColumnMetadata) column;

            columnHandles.add(new ElasticsearchColumnHandle(
                    CONNECTOR_ID,
                    column.getName(),
                    column.getType(),
                    esColumn.getJsonPath(),
                    esColumn.getJsonType(),
                    index++));
        }
        return columnHandles;
    }
}
