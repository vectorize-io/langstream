package ai.langstream.ai.agents.commons.storage.provider;

public interface StorageProviderObjectReference {

    String name();

    long size();

    String contentDigest();
}
