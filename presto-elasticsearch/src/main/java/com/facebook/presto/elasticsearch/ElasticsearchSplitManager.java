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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.elasticsearch.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final ElasticsearchClient elasticsearchClient;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchConnectorId connectorId, ElasticsearchClient elasticsearchClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.elasticsearchClient = requireNonNull(elasticsearchClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        ElasticsearchTableLayoutHandle layoutHandle = checkType(layout, ElasticsearchTableLayoutHandle.class, "partition");
        ElasticsearchTableHandle tableHandle = layoutHandle.getTable();
        ElasticsearchTable table = elasticsearchClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        for (ElasticsearchTableSource uri : table.getSources()) {
            int clmsCount = table.getColumns().size();
            splits.add(new ElasticsearchSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), uri, layoutHandle.getTupleDomain()));
        }
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
