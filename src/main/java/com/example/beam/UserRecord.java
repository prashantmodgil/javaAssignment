package com.example.beam;

import java.io.Serializable;

/** One parsed user row; serializability allows Beam to encode it between transforms. */
public record UserRecord(String id, String name, String email) implements Serializable {
    private static final long serialVersionUID = 1L;
}
