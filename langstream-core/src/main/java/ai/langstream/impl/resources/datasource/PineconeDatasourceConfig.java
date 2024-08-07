/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.impl.resources.datasource;

import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.model.Resource;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.resources.BaseDataSourceResourceProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@ResourceConfig(name = "Pinecone", description = "Connect to Pinecone service.")
public class PineconeDatasourceConfig extends BaseDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {

                @Override
                public Class getResourceConfigModelClass() {
                    return PineconeDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(
                            resource,
                            PineconeDatasourceConfig.class,
                            resource.configuration(),
                            false);
                    ConfigurationUtils.validateInteger(
                            resource.configuration(),
                            "connection-timeout-seconds",
                            1,
                            300000,
                            () -> new ClassConfigValidator.ResourceEntityRef(resource).ref());
                }
            };

    @ConfigProperty(
            description =
                    """
                            Api key for connecting to the Pinecone service.
                                    """,
            required = true)
    @JsonProperty("api-key")
    private String apiKey;

    @ConfigProperty(
            description =
                    """
                            Deprecated.
                                    """)
    private String environment;

    @ConfigProperty(
            description =
                    """
                            Deprecated.

                                    """)
    @JsonProperty("project-name")
    private String project;

    @ConfigProperty(
            description =
                    """
                            Default index name for connecting to the Pinecone service, if not specified in the agents.
                                    """)
    @JsonProperty("index-name")
    private String index;

    @ConfigProperty(
            description =
                    """
                            Deprecated.
                                    """,
            defaultValue = "10")
    @JsonProperty("server-side-timeout-sec")
    private int serverSideTimeoutSec;

    @ConfigProperty(
            description =
                    """
                            Endpoint of the Pinecone service.
                                    """)
    private String endpoint;

    @JsonProperty("connection-timeout-seconds")
    @ConfigProperty(
            defaultValue = "30",
            description =
                    """
                            Client connection timeout in seconds.
                                    """)
    private int connectionTimeoutSeconds = 30;

    @ConfigProperty(
            description =
                    """
                            Proxy for connecting to the Pinecone service. (<host>:<port>)
                                    """)
    private String proxy;
}
