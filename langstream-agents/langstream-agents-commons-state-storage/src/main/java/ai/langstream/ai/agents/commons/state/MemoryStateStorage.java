package ai.langstream.ai.agents.commons.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static ai.langstream.api.util.ConfigurationUtils.getBoolean;
import static ai.langstream.api.util.ConfigurationUtils.getString;

@Slf4j
public class MemoryStateStorage<T> implements StateStorage<T> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private T value;

    @Override
    public void store(T status) throws Exception {
        log.info("Storing state to memory");
        value = status;
    }

    @Override
    public T get(Class<T> clazz) throws Exception {
        return value;
    }
}
