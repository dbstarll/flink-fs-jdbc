package io.github.dbstarll.flink.fs.jdbc;

import io.github.dbstarll.flink.fs.jdbc.function.SizeConsumer;
import org.apache.flink.core.fs.FSDataOutputStream;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class JdbcFSDataOutputStream extends FSDataOutputStream {
    private final ByteArrayOutputStream os;
    private final SizeConsumer<? super InputStream> consumer;

    JdbcFSDataOutputStream(final int bufferSize, final SizeConsumer<? super InputStream> consumer) {
        this.os = new ByteArrayOutputStream(bufferSize);
        this.consumer = consumer;
    }

    @Override
    public long getPos() {
        return os.size();
    }

    @Override
    public void flush() throws IOException {
        os.flush();
    }

    @Override
    public void sync() {
    }

    @Override
    public void write(final int b) {
        os.write(b);
    }

    @Override
    public void write(@Nonnull final byte[] b, final int off, final int len) {
        os.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        os.close();
        try (InputStream fis = new ByteArrayInputStream(os.toByteArray())) {
            consumer.accept(fis, os.size());
        }
    }
}
