package com.scheduler.core;

import java.util.Objects;

/**
 * An input file for a job, always resolved to a storage URI.
 * Inline content bytes are a transport concern handled at the RPC boundary
 * before reaching the domain — by the time an InputFile exists, it has a URI.
 */
public record InputFile(String name, String uri) {

    public InputFile {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uri, "uri");
    }
}
