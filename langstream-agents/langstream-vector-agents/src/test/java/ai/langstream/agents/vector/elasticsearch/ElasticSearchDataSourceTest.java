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
package ai.langstream.agents.vector.elasticsearch;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class ElasticSearchDataSourceTest {
    @Test
    public void testConvertSearchRequest() {
        assertEquals(
                "SearchRequest: POST /my-index/_search?typed_keys=true {\"query\":{\"match_all\":{}}}",
                convert(
                        """
                         {"index": "my-index", "query": {"match_all": {}}}
                        """,
                        List.of()));

        assertEquals(
                "SearchRequest: POST /my-index,my-index2/_search?typed_keys=true {\"query\":{\"match_all\":{}}}",
                convert(
                        """
                         {"index": ["my-index", "my-index2"], "query": {"match_all": {}}}
                        """,
                        List.of()));
        assertEquals(
                "SearchRequest: POST /my-index,my-index2,my-index3/_search?typed_keys=true {\"query\":{\"match_all\":{}}}",
                convert(
                        """
                         {"index": ["my-index", "my-index2", ?], "query": {"match_all": {}}}
                        """,
                        List.of("my-index3")));

        assertEquals(
                "SearchRequest: POST /_search?typed_keys=true {\"query\":{\"match_all\":{}}}",
                convert(
                        """
                         {"query": {"match_all": {}}}
                        """,
                        List.of()));
    }

    private static String convert(String query, List<Object> params) {
        return ElasticSearchDataSource.ElasticSearchQueryStepDataSource.convertSearchRequest(
                        query, params)
                .toString();
    }
}
