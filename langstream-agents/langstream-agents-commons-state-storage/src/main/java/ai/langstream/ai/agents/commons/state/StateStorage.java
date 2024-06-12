package ai.langstream.ai.agents.commons.state;

public interface StateStorage<T> {

    void store(T state) throws Exception;

    T get(Class<T> clazz) throws Exception;

    String getStateReference();

}

