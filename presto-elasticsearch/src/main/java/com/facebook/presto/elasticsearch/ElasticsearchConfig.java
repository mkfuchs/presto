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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class ElasticsearchConfig
{
    private URI metadata;
    private String hostAddress;
    private String port;
    private String schemaName;

    @NotNull
    public URI getMetadata()
    {
        return metadata;
    }

    @Config("elasticsearch.metadata-uri")
    public ElasticsearchConfig setMetadata(URI metadata)
    {
        this.metadata = metadata;
        return this;
    }

    @NotNull
    public String getHostAddress()
    {
        return hostAddress;
    }

    @Config("elasticsearch.hostAddress")
    public ElasticsearchConfig setHostAddress(String hostAddress)
    {
        this.hostAddress = hostAddress;
        return this;
    }

    @NotNull
    public String getPort()
    {
        return port;
    }

    @Config("elasticsearch.port")
    public ElasticsearchConfig setPort(String port)
    {
        this.port = port;
        return this;
    }

    @NotNull
    public String getSchemaName()
    {
        return schemaName;
    }

    @Config("elasticsearch.schemaName")
    public ElasticsearchConfig setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return this;
    }

}
