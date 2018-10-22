package io.vasto.exception;

public class NoClusterFoundException extends RuntimeException {
    public NoClusterFoundException(String message) {
        super(message);
    }
}
