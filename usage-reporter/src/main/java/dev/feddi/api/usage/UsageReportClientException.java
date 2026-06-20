package dev.feddi.api.usage;

import org.jspecify.annotations.Nullable;

public class UsageReportClientException extends RuntimeException {

    private final int statusCode;

    public UsageReportClientException(String message) {
        this(message, -1, null);
    }

    public UsageReportClientException(String message, @Nullable Throwable cause) {
        this(message, -1, cause);
    }

    public UsageReportClientException(String message, int statusCode) {
        this(message, statusCode, null);
    }

    public UsageReportClientException(String message, int statusCode, @Nullable Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }
}
