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
package ai.langstream.api.runner.code;

import lombok.Getter;

@Getter
public enum SystemHeaders {
    SERVICE_REQUEST_ID_HEADER("langstream-service-request-id"),
    ERROR_HANDLING_ERROR_MESSAGE("langstream-error-message"),
    ERROR_HANDLING_ERROR_MESSAGE_LEGACY("error-msg"),
    ERROR_HANDLING_ERROR_CLASS("langstream-error-class"),
    ERROR_HANDLING_ERROR_CLASS_LEGACY("error-class"),
    ERROR_HANDLING_ERROR_TYPE("langstream-error-type"),

    ERROR_HANDLING_CAUSE_ERROR_MESSAGE("langstream-error-cause-message"),
    ERROR_HANDLING_CAUSE_ERROR_MESSAGE_LEGACY("cause-msg"),
    ERROR_HANDLING_CAUSE_ERROR_CLASS("langstream-error-cause-class"),
    ERROR_HANDLING_CAUSE_ERROR_CLASS_LEGACY("cause-class"),

    ERROR_HANDLING_ROOT_CAUSE_ERROR_MESSAGE("langstream-error-root-cause-message"),
    ERROR_HANDLING_ROOT_CAUSE_ERROR_MESSAGE_LEGACY("root-cause-msg"),
    ERROR_HANDLING_ROOT_CAUSE_ERROR_CLASS("langstream-error-root-cause-class"),
    ERROR_HANDLING_ROOT_CAUSE_ERROR_CLASS_LEGACY("root-cause-class"),

    ERROR_HANDLING_SOURCE_TOPIC("langstream-error-source-topic"),
    ERROR_HANDLING_SOURCE_TOPIC_LEGACY("source-topic");

    private final String key;

    SystemHeaders(String key) {
        this.key = key;
    }
}
