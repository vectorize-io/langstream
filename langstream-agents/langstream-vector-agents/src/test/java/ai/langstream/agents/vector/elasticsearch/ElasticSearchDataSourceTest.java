package ai.langstream.agents.vector.elasticsearch;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ElasticSearchDataSourceTest {
    @Test
    public void testConvertSearchRequest() {
        assertEquals("SearchRequest: POST /my-index/_search?typed_keys=true {\"query\":{\"match_all\":{}}}",
                convert("""
                         {"index": "my-index", "query": {"match_all": {}}}
                        """, List.of())
        );

        assertEquals("SearchRequest: POST /my-index,my-index2/_search?typed_keys=true {\"query\":{\"match_all\":{}}}",
                convert("""
                         {"index": ["my-index", "my-index2"], "query": {"match_all": {}}}
                        """, List.of())
        );
        assertEquals("SearchRequest: POST /my-index,my-index2,my-index3/_search?typed_keys=true {\"query\":{\"match_all\":{}}}",
                convert("""
                         {"index": ["my-index", "my-index2", ?], "query": {"match_all": {}}}
                        """, List.of("my-index3"))
        );

        assertEquals("SearchRequest: POST /_search?typed_keys=true {\"query\":{\"match_all\":{}}}",
                convert("""
                         {"query": {"match_all": {}}}
                        """, List.of())
        );
    }

    private static String convert(String query, List<Object> params) {
        return ElasticSearchDataSource.ElasticSearchQueryStepDataSource.convertSearchRequest(
                        query, params)
                .toString();
    }

}