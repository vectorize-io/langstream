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
