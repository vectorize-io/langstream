package ai.langstream.deployer.k8s.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SpecDifferTest {

    @Test
    void testJsonOrderAndNulls() throws Exception {
        String json1 =
                """
                {
                  "c": 3,
                  "a": 1,
                  "b": null
                }""";

        JSONComparator.Result result = SpecDiffer.generateDiff(json1, Map.of("a", 1, "c", 3));
        SpecDiffer.logDetailedSpecDiff(result);
        assertTrue(result.areEquals());
    }
}
