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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.json.JSONArray;

//import java.util.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class ElasticsearchRecordCursor
        implements RecordCursor
{
    private final List<ElasticsearchColumnHandle> columnHandles;
    private final Map<String, Integer> jsonPathToIndex;
    private final Iterator<SearchHit> lines;
    private long totalBytes;
    private List<Object> fields;
    private final boolean isFieldQuery;

    public ElasticsearchRecordCursor(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchSplit split, ElasticsearchClient elasticsearchClient)
    {
        this.columnHandles = columnHandles;
        this.jsonPathToIndex = new HashMap();
        this.totalBytes = 0;

        for (int i = 0; i < columnHandles.size(); i++) {
            this.jsonPathToIndex.put(columnHandles.get(i).getColumnJsonPath(), i);
        }

        // in elasticsearch when there is nested types it is not possible to add fields in queries
        this.isFieldQuery = columnHandles
                .stream()
                .filter((c) -> c.getColumnJsonType().equals("nested"))
                .count() == 0;

        this.lines = getRows(new ElasticsearchQueryBuilder(columnHandles, split, elasticsearchClient, isFieldQuery)).iterator();
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        SearchHit hit = lines.next();

        fields = new ArrayList(Collections.nCopies(columnHandles.size(), null));

        setFieldIfExists("_id", hit.getId());
        setFieldIfExists("_index", hit.getIndex());

        if (isFieldQuery) {
            extractFromHitField(hit);
        }
        else {
            extractFromSource(hit);
        }

        totalBytes += fields.size();

        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        String s = getFieldValue(field).toString();

        if (s == null || s.equalsIgnoreCase("") || s.equalsIgnoreCase("false") || s.equalsIgnoreCase("off") || s.equalsIgnoreCase("no") || s.equalsIgnoreCase("0")) {
            return false;
        }
        else {
            return true;
        }
    }

    @Override
    public long getLong(int field)
    {
        //checkFieldType(field, BIGINT);
        return Long.valueOf(String.valueOf(getFieldValue(field)));
    }

    @Override
    public double getDouble(int field)
    {
        //checkFieldType(field, DOUBLE);
        return Double.valueOf(String.valueOf(getFieldValue(field)));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);

        Object value = getFieldValue(field);
        if (value instanceof Collection) {
            return Slices.utf8Slice(String.valueOf(toJson((List<Map<String, Object>>) value)));
        }
        else {
            return Slices.utf8Slice(String.valueOf(value));
        }
    }

    @Override
    public Object getObject(int field)
    {
        return null;
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getFieldValue(field) == null;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }

    private List<SearchHit> getRows(ElasticsearchQueryBuilder queryBuilder)
    {
        List<SearchHit> result = new ArrayList<>();

        SearchResponse scrollResp = queryBuilder
                .buildScrollSearchRequest()
                .execute()
                .actionGet();

        //Scroll until no hits are returned
        while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                result.add(hit);
            }

            scrollResp = queryBuilder
                    .prepareSearchScroll(scrollResp.getScrollId())
                    .execute()
                    .actionGet();

            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

        return result;
    }

    private void setFieldIfExists(String key, Object value)
    {
        if (jsonPathToIndex.containsKey(key)) {
            fields.set(jsonPathToIndex.get(key), value);
        }
    }

    private Object getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }

    private void extractFromHitField(SearchHit hit)
    {
        Map<String, SearchHitField> map = hit.getFields();
        for (Map.Entry<String, SearchHitField> entry : map.entrySet()) {
            String jsonPath = entry.getKey().toString();
            Object entryValue = entry.getValue().getValue();

            setFieldIfExists(jsonPath, entryValue);
        }
    }

    private void extractFromSource(SearchHit hit)
    {
        Map<String, Object> map = hit.getSource();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String jsonPath = entry.getKey().toString();
            Object entryValue = entry.getValue();

            setFieldIfExists(jsonPath, entryValue);
        }
    }

    private String toJson(Collection<Map<String, Object>> list)
    {
        return new JSONArray(list).toString();
    }
}
