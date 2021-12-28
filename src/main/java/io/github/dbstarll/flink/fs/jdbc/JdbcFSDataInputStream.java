package io.github.dbstarll.flink.fs.jdbc;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class JdbcFSDataInputStream extends FSDataInputStream {
    private final ByteArrayInputStream is;
    private final long size;

    JdbcFSDataInputStream(final int bufferSize, final InputStream is, final long size) throws IOException {
        this.is = copy(bufferSize, is);
        this.size = size;
    }

    private static ByteArrayInputStream copy(final int bufferSize, final InputStream is) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream(bufferSize)) {
            IOUtils.copyBytes(is, os, bufferSize, true);
            return new ByteArrayInputStream(os.toByteArray());
        }
    }

    @Override
    public void seek(final long desired) {
        is.reset();
        //noinspection ResultOfMethodCallIgnored
        is.skip(desired);
    }

    @Override
    public long getPos() {
        return size - available();
    }

    @Override
    public int read() {
        return is.read();
    }

    @Override
    public int read(@Nonnull final byte[] b, final int off, final int len) {
        return is.read(b, off, len);
    }

    @Override
    public long skip(final long n) {
        return is.skip(n);
    }

    @Override
    public int available() {
        return is.available();
    }

    @Override
    public void close() throws IOException {
        this.is.close();
    }
}
