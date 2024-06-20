package ai.langstream.ai.agents.commons.storage.provider;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
public class StorageProviderSourceState {

    private Map<String, String> allTimeObjects = new HashMap<>();
}
