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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

public class ElasticsearchQueryBuilder
{
    private static final Logger log = Logger.get(ElasticsearchQueryBuilder.class);

    private static final int SCROLL_TIME = 60000;
    private static final int SCROLL_SIZE = 5000;

    final Client client;
    final TupleDomain<ColumnHandle> tupleDomain;
    final List<ElasticsearchColumnHandle> columns;
    final boolean isToAddFields;

    private final String index;
    private final String type;

    public ElasticsearchQueryBuilder(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchSplit split, ElasticsearchClient elasticsearchClient, boolean isToAddFields)
    {
        ElasticsearchTableSource tableSource = split.getUri();
        String clusterName = tableSource.getClusterName();
        String hostAddress = tableSource.getHostAddress();
        int port = tableSource.getPort();
        this.index = tableSource.getIndex();
        this.type = tableSource.getType();

        log.debug(String.format("Connecting to cluster %s from %s:%d, index %s, type %s", clusterName, hostAddress, port, index, type));
        this.client = elasticsearchClient.getInternalClients().get(clusterName);

        this.tupleDomain = split.getTupleDomain();
        this.columns = columnHandles;

        this.isToAddFields = isToAddFields;
    }

    public SearchRequestBuilder buildScrollSearchRequest()
    {
        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch(index != null && !index.isEmpty() ? index : "_all")
                .setTypes(type)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(SCROLL_TIME))
                .setQuery(getSearchQuery())
                .setSize(SCROLL_SIZE); // per shard

        // elasticsearch doesn't support adding fields when there is a nested type
        if (isToAddFields) {
            searchRequestBuilder.addFields(columns
                    .stream()
                    .map((c) -> c.getColumnJsonPath())
                    .toArray(size -> new String[size]));
        }

        return searchRequestBuilder;
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId)
    {
        return client
                .prepareSearchScroll(scrollId)
                .setScroll(new TimeValue(SCROLL_TIME));
    }

    private FilteredQueryBuilder getSearchQuery()
    {
        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();

        for (ElasticsearchColumnHandle column : columns) {
            Type type = column.getColumnType();
            tupleDomain
                    .getDomains()
                    .ifPresent((e) -> {
                        Domain domain = e.get(column);
                        if (domain != null) {
                            boolFilterBuilder.must(addFilter(column.getColumnJsonPath(), domain, type));
                        }
                    });
        }

        return QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(),
                boolFilterBuilder.hasClauses()
                        ? boolFilterBuilder
                        : FilterBuilders.matchAllFilter());
    }

    private BoolFilterBuilder addFilter(String columnName, Domain domain, Type type)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();

        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            boolFilterBuilder.must(FilterBuilders.missingFilter(columnName));
        }
        else if (domain.getValues().isAll()) {
            boolFilterBuilder.must(FilterBuilders.existsFilter(columnName));
        }
        else {
            List<Object> singleValues = new ArrayList<>();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                checkState(!range.isAll()); // Already checked
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                boolFilterBuilder.must(FilterBuilders.rangeFilter(columnName).gt(getValue(type, range.getLow().getValue())));
                                break;
                            case EXACTLY:
                                boolFilterBuilder.must(FilterBuilders.rangeFilter(columnName).gte(getValue(type, range.getLow().getValue())));
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                boolFilterBuilder.must(FilterBuilders.rangeFilter(columnName).lte(getValue(type, range.getHigh().getValue())));
                                break;
                            case BELOW:
                                boolFilterBuilder.must(FilterBuilders.rangeFilter(columnName).lt(getValue(type, range.getHigh().getValue())));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                }
            }

            if (singleValues.size() == 1) {
                boolFilterBuilder.must(FilterBuilders.termFilter(columnName, getValue(type, getOnlyElement(singleValues))));
            }
        }

        return boolFilterBuilder;
    }

    private Object getValue(Type type, Object value)
    {
        if (type.equals(BigintType.BIGINT)) {
            return (long) value;
        }
        else if (type.equals(IntegerType.INTEGER)) {
            return ((Number) value).intValue();
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return (double) value;
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            return ((Slice) value).toStringUtf8();
        }
        else if (type.equals(BooleanType.BOOLEAN)) {
            return (boolean) value;
        }
        else {
            throw new UnsupportedOperationException("Query Builder can't handle type: " + type);
        }
    }
}
