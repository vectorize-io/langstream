package ai.langstream.ai.agents.commons.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

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

    @Override
    public String getStateReference() {
        return null;
    }
}
