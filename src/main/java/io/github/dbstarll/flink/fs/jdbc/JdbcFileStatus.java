package io.github.dbstarll.flink.fs.jdbc;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.LocatedFileStatus;
import org.apache.flink.core.fs.Path;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringJoiner;

public final class JdbcFileStatus implements LocatedFileStatus {
    private final long id;
    private final long parent;
    private final Path path;
    private final boolean dir;
    private final long len;
    private final long created;
    private final long modified;

    private JdbcFileStatus(final long id, final long parent, final Path path, final boolean dir,
                           final long len, final long created, final long modified) {
        this.id = id;
        this.parent = parent;
        this.path = path;
        this.dir = dir;
        this.len = len;
        this.created = created;
        this.modified = modified;
    }

    static JdbcFileStatus file(final FileSystem fs, final long id, final long parent, final String path,
                               final long len, final long created, final long modified) {
        return new JdbcFileStatus(
                id, parent,
                new Path(fs.getUri().getScheme() + "://" + fs.getUri().getAuthority() + path),
                false, len,
                created, modified
        );
    }

    static JdbcFileStatus dir(final FileSystem fs, final long id, final long parent, final String path,
                              final long created, final long modified) {
        return new JdbcFileStatus(
                id, parent,
                new Path(fs.getUri().getScheme() + "://" + fs.getUri().getAuthority() + path),
                true, 0,
                created, modified
        );
    }

    static JdbcFileStatus root(final FileSystem fs) {
        return dir(fs, 0, 0, "/", 0, 0);
    }

    static JdbcFileStatus rs(final FileSystem fs, final ResultSet rs) throws SQLException {
        if (rs.getBoolean("file")) {
            return file(fs,
                    rs.getLong("id"),
                    rs.getLong("parent"),
                    rs.getString("path"),
                    rs.getLong("len"),
                    rs.getLong("created"),
                    rs.getLong("modified")
            );
        } else {
            return dir(fs,
                    rs.getLong("id"),
                    rs.getLong("parent"),
                    rs.getString("path"),
                    rs.getLong("created"),
                    rs.getLong("modified")
            );
        }
    }

    @Override
    public long getLen() {
        return len;
    }

    @Override
    public long getBlockSize() {
        return len;
    }

    @Override
    public short getReplication() {
        return 1;
    }

    @Override
    public long getModificationTime() {
        return modified;
    }

    @Override
    public long getAccessTime() {
        return 0;
    }

    @Override
    public boolean isDir() {
        return dir;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public BlockLocation[] getBlockLocations() {
        return new BlockLocation[]{new JdbcBlockLocation(len)};
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", JdbcFileStatus.class.getSimpleName() + "[", "]")
                .add("id=" + id)
                .add("parent=" + parent)
                .add("path=" + path)
                .add("dir=" + dir)
                .add("len=" + len)
                .add("created=" + created)
                .add("modified=" + modified)
                .toString();
    }

    long getId() {
        return id;
    }

    long getParent() {
        return parent;
    }
}
