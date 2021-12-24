package io.github.dbstarll.flink.fs.jdbc.function;

import java.io.IOException;
import java.sql.SQLException;

@FunctionalInterface
public interface Function<T, R> {
    /**
     * Applies this function to the given argument.
     *
     * @param t the function argument
     * @return the function result
     * @throws SQLException sql异常
     * @throws IOException  io异常
     */
    R apply(T t) throws SQLException, IOException;
}
