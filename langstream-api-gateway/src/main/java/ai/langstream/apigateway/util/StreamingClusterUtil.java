package ai.langstream.apigateway.util;

import ai.langstream.api.model.StreamingCluster;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;

public class StreamingClusterUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    public static String asKey(StreamingCluster streamingCluster) {
        return mapper.writeValueAsString(
                Pair.of(streamingCluster.type(), streamingCluster.configuration()));
    }
}
