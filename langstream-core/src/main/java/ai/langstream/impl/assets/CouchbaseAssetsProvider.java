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
package ai.langstream.impl.assets;

import ai.langstream.api.doc.AssetConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.impl.common.AbstractAssetProvider;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseAssetsProvider extends AbstractAssetProvider {

    public CouchbaseAssetsProvider() {
        super(Set.of("couchbase-assets"));
    }

    @Override
    protected Class getAssetConfigModelClass(String type) {
        return InstanceConfig.class;
    }

    @Override
    protected boolean lookupResource(String fieldName) {
        return "datasource".equals(fieldName);
    }

    @AssetConfig(
            name = "Couchbase bucket, scope, collection and search index",
            description =
                    """
                    Manage a couchbase instance.
                    """)
    @Data
    public static class InstanceConfig {

        @ConfigProperty(
                description =
                        """
                       Reference to a datasource id configured in the application.
                       """,
                required = true)
        private String datasource;

        @ConfigProperty(
                required = true,
                description =
                        """
                          Username for couchbase.
                       """)
        private int username;

        @ConfigProperty(
                required = true,
                description =
                        """
                          Password for couchbase.
                       """)
        private int password;

        @ConfigProperty(
                required = true,
                description =
                        """
                          Connection string for the instance.
                       """)
        private int connection_string;

        @ConfigProperty(
                required = true,
                description =
                        """
                          The name of the bucket.
                       """)
        private int bucket;

        @ConfigProperty(
                required = true,
                description =
                        """
                          The name of the scope.
                       """)
        private int scope;

        @ConfigProperty(
                required = true,
                description =
                        """
                          The name of the collection.
                       """)
        private int collection;

        @ConfigProperty(
                required = true,
                description =
                        """
                          The name of the port.
                       """)
        private int port;

        @ConfigProperty(
                required = true,
                description =
                        """
                          The vector dimension size.
                       """)
        private int dimension;
    }

    @Data
    public static class Statement {
        String api;
        String method;
        String body;
    }
}
