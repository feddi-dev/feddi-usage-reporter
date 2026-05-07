package dev.feddi.api.usage;

public class UsageReportClientException extends RuntimeException {

    private final int statusCode;

    public UsageReportClientException(String message) {
        this(message, -1, null);
    }

    public UsageReportClientException(String message, Throwable cause) {
        this(message, -1, cause);
    }

    public UsageReportClientException(String message, int statusCode) {
        this(message, statusCode, null);
    }

    public UsageReportClientException(String message, int statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }
}
