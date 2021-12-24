package io.github.dbstarll.flink.fs.jdbc.function;

import java.io.IOException;

@FunctionalInterface
public interface SizeConsumer<T> {
    /**
     * Performs this operation on the given argument.
     *
     * @param t    the input argument1
     * @param size size
     * @throws IOException io异常
     */
    void accept(T t, long size) throws IOException;
}
