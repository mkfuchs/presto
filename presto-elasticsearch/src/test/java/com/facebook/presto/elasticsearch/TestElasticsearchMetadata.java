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

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;

import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.CONNECTOR_ID;
import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_SCHEMA1;
import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_SCHEMA2;
import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_TBL_1;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestElasticsearchMetadata
{
    private static final ElasticsearchTableHandle ES_TABLE_HANDLE = new ElasticsearchTableHandle(CONNECTOR_ID, ES_SCHEMA1, ES_TBL_1);
    private ElasticsearchMetadata metadata;
    private URI metadataUri;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestElasticsearchClient.class, "/example-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        metadataUri = metadataUrl.toURI();
        ElasticsearchClient client = new ElasticsearchClient(new ElasticsearchConfig().setMetadata(metadataUri), MetadataUtil.CATALOG_CODEC);
        metadata = new ElasticsearchMetadata(new ElasticsearchConnectorId(CONNECTOR_ID), client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableSet.of(ES_SCHEMA1, ES_SCHEMA2));
    }

    @Test
    public void testGetTableHandle()
    {
        assertEquals(
                metadata.getTableHandle(SESSION, new SchemaTableName(ES_SCHEMA1, ES_TBL_1)),
                ES_TABLE_HANDLE);
    }
}
