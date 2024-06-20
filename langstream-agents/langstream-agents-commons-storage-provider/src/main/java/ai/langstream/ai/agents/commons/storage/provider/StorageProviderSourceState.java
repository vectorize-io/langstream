package ai.langstream.ai.agents.commons.storage.provider;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class StorageProviderSourceState {

    private Map<String, String> allTimeObjects = new HashMap<>();
}
