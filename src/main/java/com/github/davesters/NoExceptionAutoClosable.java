package com.github.davesters;

/**
 * An AutoClosable version with no exception on the close method.
 */
public interface NoExceptionAutoClosable extends AutoCloseable {
    @Override
    void close();
}
