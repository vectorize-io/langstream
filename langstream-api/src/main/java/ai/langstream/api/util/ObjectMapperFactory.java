package ai.langstream.api.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectMapperFactory {

    private static final ObjectMapper MAPPER = configureObjectMapper(new ObjectMapper());
    private static final ObjectMapper PRETTY_PRINT_MAPPER =
            MAPPER.copy().configure(SerializationFeature.INDENT_OUTPUT, true);

    private static final ObjectMapper YAML_MAPPER =
            configureObjectMapper(
                    new ObjectMapper(
                            YAMLFactory.builder()
                                    .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                                    .disable(YAMLGenerator.Feature.SPLIT_LINES)
                                    .build()));

    public static ObjectMapper getDefaultMapper() {
        return MAPPER;
    }

    public static ObjectMapper getPrettyPrintMapper() {
        return PRETTY_PRINT_MAPPER;
    }

    public static ObjectMapper getDefaultYamlMapper() {
        return YAML_MAPPER;
    }

    private static ObjectMapper configureObjectMapper(ObjectMapper mapper) {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true);
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        return mapper;
    }
}
