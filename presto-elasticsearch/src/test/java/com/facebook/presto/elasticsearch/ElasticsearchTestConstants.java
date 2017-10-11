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

public final class ElasticsearchTestConstants
{
    public static final String ES_SCHEMA1 = "es_schema1";
    public static final String ES_SCHEMA2 = "es_schema2";
    public static final String ES_TBL_1 = "fancyPantsTable";
    public static final String ES_TBL_2 = "fancyPantsTable2";
    public static final String CONNECTOR_ID = "TEST";

    private ElasticsearchTestConstants()
    {
        throw new AssertionError();
    }
}
