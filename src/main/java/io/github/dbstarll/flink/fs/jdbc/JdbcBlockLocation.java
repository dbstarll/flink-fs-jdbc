package io.github.dbstarll.flink.fs.jdbc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nonnull;

@Internal
public final class JdbcBlockLocation implements BlockLocation {
    private final long length;

    JdbcBlockLocation(final long length) {
        this.length = length;
    }

    @Override
    public String[] getHosts() {
        return StringUtils.EMPTY_STRING_ARRAY;
    }

    @Override
    public long getOffset() {
        return 0;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    public int compareTo(@Nonnull final BlockLocation o) {
        return 0;
    }
}
