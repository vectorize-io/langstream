/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.impl.storage.k8s.codestorage;

import ai.langstream.api.codestorage.CodeStorage;
import ai.langstream.api.codestorage.CodeStorageProvider;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzureBlobCodeStorageProvider implements CodeStorageProvider {

    @Override
    public CodeStorage createImplementation(
            String codeStorageType, Map<String, Object> configuration) {
        return new AzureBlobCodeStorage(configuration);
    }

    @Override
    public boolean supports(String codeStorageType) {
        return "azure".equals(codeStorageType);
    }
}
