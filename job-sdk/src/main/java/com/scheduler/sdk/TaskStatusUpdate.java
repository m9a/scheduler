package com.scheduler.sdk;

import java.util.Objects;

/**
 * Status update sent from JobProcess (in the job process) to WorkerAgent (in the worker JVM)
 * via HTTP POST to {@code /task-status}.
 */
public record TaskStatusUpdate(String jobId, int taskIndex, String taskName,
                               TaskStatus status, String errorMessage,
                               long durationMs, String output) {

    public TaskStatusUpdate {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(taskName);
        Objects.requireNonNull(status);
    }

    /** Convenience constructor for updates without duration/output (e.g. RUNNING). */
    public TaskStatusUpdate(String jobId, int taskIndex, String taskName, TaskStatus status, String errorMessage) {
        this(jobId, taskIndex, taskName, status, errorMessage, 0, null);
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        appendField(sb, "jobId", jobId);
        sb.append(",\"taskIndex\":").append(taskIndex);
        appendField(sb, ",taskName", taskName);
        appendField(sb, ",status", status.name());
        if (errorMessage != null) {
            appendField(sb, ",errorMessage", errorMessage);
        }
        sb.append(",\"durationMs\":").append(durationMs);
        if (output != null) {
            appendField(sb, ",output", output);
        }
        sb.append("}");
        return sb.toString();
    }

    private static void appendField(StringBuilder sb, String name, String value) {
        if (name.startsWith(",")) {
            sb.append(",");
            name = name.substring(1);
        }
        sb.append("\"").append(name).append("\":\"").append(escapeJson(value)).append("\"");
    }

    public static TaskStatusUpdate fromJson(String json) {
        String jobId = extractString(json, "jobId");
        int taskIndex = extractInt(json, "taskIndex");
        String taskName = extractString(json, "taskName");
        TaskStatus status = TaskStatus.valueOf(extractString(json, "status"));
        String errorMessage = json.contains("\"errorMessage\"") ? extractString(json, "errorMessage") : null;
        long durationMs = json.contains("\"durationMs\"") ? extractLong(json, "durationMs") : 0;
        String output = json.contains("\"output\"") ? extractString(json, "output") : null;
        return new TaskStatusUpdate(jobId, taskIndex, taskName, status, errorMessage, durationMs, output);
    }

    private static String extractString(String json, String field) {
        String prefix = "\"" + field + "\":\"";
        int start = json.indexOf(prefix);
        if (start == -1) {
            throw new IllegalArgumentException("Field '" + field + "' not found in: " + json);
        }
        start += prefix.length();
        int end = json.indexOf("\"", start);
        return unescapeJson(json.substring(start, end));
    }

    private static int extractInt(String json, String field) {
        return (int) extractLong(json, field);
    }

    private static long extractLong(String json, String field) {
        String prefix = "\"" + field + "\":";
        int start = json.indexOf(prefix);
        if (start == -1) {
            throw new IllegalArgumentException("Field '" + field + "' not found in: " + json);
        }
        start += prefix.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        return Long.parseLong(json.substring(start, end));
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static String unescapeJson(String value) {
        return value.replace("\\t", "\t")
                .replace("\\r", "\r")
                .replace("\\n", "\n")
                .replace("\\\"", "\"")
                .replace("\\\\", "\\");
    }
}
