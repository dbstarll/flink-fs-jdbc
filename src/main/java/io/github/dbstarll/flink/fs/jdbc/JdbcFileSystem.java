package io.github.dbstarll.flink.fs.jdbc;

import io.github.dbstarll.flink.fs.jdbc.function.Function;
import io.github.dbstarll.flink.fs.jdbc.function.SizeConsumer;
import org.apache.flink.core.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemLoopException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public final class JdbcFileSystem extends FileSystem {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcFileSystem.class);

    private final DataSource dataSource;
    private final int defaultBufferSize;
    private final URI fsUri;
    private final JdbcFileStatus root;

    private final String table;
    private final String sqlGetByPath;
    private final String sqlGetDataByPath;
    private final String sqlFindByParent;
    private final String sqlInsert;
    private final String sqlUpdateData;
    private final String sqlUpdateModified;
    private final String sqlMove;
    private final String sqlMoveSub;
    private final String sqlDeleteById;
    private final String sqlDeleteByPath;

    JdbcFileSystem(final DataSource dataSource, final int defaultBufferSize, final URI fsUri) {
        this.dataSource = dataSource;
        this.defaultBufferSize = defaultBufferSize;
        this.fsUri = fsUri;
        this.root = JdbcFileStatus.root(this);
        this.table = fsUri.getAuthority();
        this.sqlGetByPath = "SELECT id,parent,path,file,len,created,modified FROM `" + table + "` WHERE path=?";
        this.sqlGetDataByPath = "SELECT data,len FROM `" + table + "` WHERE path=? and file=1";
        this.sqlFindByParent = "SELECT id,parent,path,file,len,created,modified FROM `" + table + "` WHERE parent=?";
        this.sqlInsert = "INSERT INTO `" + table + "` (parent,name,path,file,created,modified) VALUES (?,?,?,?,?,?)";
        this.sqlUpdateData = "UPDATE `" + table + "` SET data=?,len=?,modified=? WHERE id=? and file=1";
        this.sqlUpdateModified = "UPDATE `" + table + "` SET modified=? WHERE id=? and file=0";
        this.sqlMove = "UPDATE `" + table + "` SET parent=?,name=?,path=? WHERE id=? and parent=? and name=?";
        this.sqlMoveSub = "UPDATE `" + table + "` SET path=concat(?,SUBSTRING(path,?)) WHERE path LIKE ?";
        this.sqlDeleteById = "DELETE FROM `" + table + "` WHERE id=?";
        this.sqlDeleteByPath = "DELETE FROM `" + table + "` WHERE path=? OR path LIKE ?";
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(fsUri);
    }

    @Override
    public Path getHomeDirectory() {
        return new Path(fsUri);
    }

    @Override
    public URI getUri() {
        return fsUri;
    }

    @Override
    public FileStatus getFileStatus(final Path f) throws IOException {
        checkPath(f);
        if (!f.isAbsolute()) {
            return getFileStatus(new Path(getWorkingDirectory(), f));
        } else if (f.getParent() == null) {
            return root;
        }
        return connection(false, conn -> {
            final FileStatus status = getFileStatus(conn, f);
            if (status == null) {
                throw new FileNotFoundException(f.toString());
            } else {
                return status;
            }
        }, "getFileStatus", f.toString());
    }

    private JdbcFileStatus getFileStatus(final Connection conn, final Path f) throws IOException {
        if (f.getParent() == null) {
            return root;
        }
        return statement(conn, sqlGetByPath, false, ps -> {
            ps.setString(1, f.getPath());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return JdbcFileStatus.rs(this, rs);
                }
                return null;
            }
        });
    }

    @Override
    public BlockLocation[] getFileBlockLocations(final FileStatus file,
                                                 final long start, final long len) throws IOException {
        if (file instanceof JdbcFileStatus) {
            return ((JdbcFileStatus) file).getBlockLocations();
        }
        throw new IOException("File status does not belong to the JdbcFileStatus: " + file);
    }

    @Override
    public FSDataInputStream open(final Path f) throws IOException {
        return open(f, defaultBufferSize);
    }

    @Override
    public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
        checkPath(f);
        if (!f.isAbsolute()) {
            return open(new Path(getWorkingDirectory(), f));
        }
        return connection(false, conn -> {
            final FSDataInputStream is = open(conn, f, bufferSize);
            if (is == null) {
                throw new FileNotFoundException(f.toString());
            } else {
                return is;
            }
        }, "open", f.toString());
    }

    private FSDataInputStream open(final Connection conn, final Path f, final int bufferSize) throws IOException {
        return statement(conn, sqlGetDataByPath, false, ps -> {
            ps.setString(1, f.getPath());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new JdbcFSDataInputStream(bufferSize,
                            rs.getBlob("data").getBinaryStream(),
                            rs.getLong("len"));
                }
                return null;
            }
        });
    }

    @Override
    public FileStatus[] listStatus(final Path f) throws IOException {
        checkPath(f);
        if (!f.isAbsolute()) {
            return listStatus(new Path(getWorkingDirectory(), f));
        }
        return connection(false, conn -> listStatus(conn, f), "listStatus", f.toString());
    }

    private FileStatus[] listStatus(final Connection conn, final Path f) throws IOException {
        final JdbcFileStatus status = getFileStatus(conn, f);
        if (status == null) {
            throw new FileNotFoundException(f.toString());
        } else if (!status.isDir()) {
            return new FileStatus[]{status};
        }
        return statement(conn, sqlFindByParent, false, ps -> {
            ps.setLong(1, status.getId());
            final List<FileStatus> statuses = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    statuses.add(JdbcFileStatus.rs(this, rs));
                }
            }
            return statuses.toArray(new FileStatus[0]);
        });
    }

    @Override
    public boolean delete(final Path f, final boolean recursive) throws IOException {
        checkPath(f);
        if (!f.isAbsolute()) {
            return mkdirs(new Path(getWorkingDirectory(), f));
        } else if (f.getParent() == null) {
            throw new IOException("root dir can not delete.");
        }
        return connection(true, conn -> delete(conn, f, recursive) > 0,
                "delete", Boolean.toString(recursive), f.toString());
    }

    private int delete(final Connection conn, final Path f, final boolean recursive) throws IOException {
        final JdbcFileStatus status = getFileStatus(conn, f);
        if (status == null) {
            throw new FileNotFoundException(f.toString());
        } else if (!status.isDir()) {
            // delete file
            final int count = statement(conn, sqlDeleteById, false, ps -> {
                ps.setLong(1, status.getId());
                return ps.executeUpdate();
            });
            LOGGER.info("delete file[" + count + "]: " + f.toString());
            return count;
        } else if (recursive) {
            // 递归删除目录
            final int count = statement(conn, sqlDeleteByPath, false, ps -> {
                ps.setString(1, f.getPath());
                ps.setString(2, f.getPath() + "/%");
                return ps.executeUpdate();
            });
            LOGGER.info("delete dir recursive[" + count + "]: " + f.toString());
            return count;
        } else if (listStatus(conn, f).length == 0) {
            // delete empty dir
            final int count = statement(conn, sqlDeleteById, false, ps -> {
                ps.setLong(1, status.getId());
                return ps.executeUpdate();
            });
            LOGGER.info("delete dir[" + count + "]: " + f.toString());
            return count;
        } else {
            throw new DirectoryNotEmptyException(f.toString());
        }
    }

    @Override
    public boolean mkdirs(final Path f) throws IOException {
        checkPath(f);
        if (!f.isAbsolute()) {
            return mkdirs(new Path(getWorkingDirectory(), f));
        }
        return connection(true, conn -> mkdirs(conn, f) != null, "mkdirs", f.toString());
    }

    private JdbcFileStatus mkdirs(final Connection conn, final Path f) throws IOException {
        final JdbcFileStatus status = getFileStatus(conn, f);
        if (status == null) {
            final Path parent = f.getParent();
            final JdbcFileStatus parentStatus = parent != null ? mkdirs(conn, parent) : root;
            if (f.getName().length() == 0) {
                return parentStatus;
            } else {
                LOGGER.info("mkdir: " + f);
                final JdbcFileStatus ns = insert(conn, f, parentStatus.getId(), false);
                if (parent != null) {
                    //更新父目录的修改时间
                    updateModified(conn, parentStatus, ns.getModificationTime());
                }
                return ns;
            }
        } else if (status.isDir()) {
            return status;
        } else {
            throw new FileAlreadyExistsException(f.toString());
        }
    }

    @Override
    public FSDataOutputStream create(final Path f, final WriteMode overwriteMode) throws IOException {
        checkPath(f);
        if (!f.isAbsolute()) {
            return create(new Path(getWorkingDirectory(), f), overwriteMode);
        }
        return connection(true, conn -> create(conn, f, overwriteMode), "create", f.toString());
    }

    private FSDataOutputStream create(final Connection conn, final Path f, final WriteMode mode) throws IOException {
        final JdbcFileStatus status = getFileStatus(conn, f);
        if (status == null) {
            if (f.getName().length() == 0) {
                throw new IOException("file name not set.");
            }
            final Path parent = f.getParent();
            final JdbcFileStatus parentStatus = parent != null ? mkdirs(conn, parent) : root;
            LOGGER.info("create: " + f);
            final JdbcFileStatus ns = insert(conn, f, parentStatus.getId(), true);
            if (parent != null) {
                //更新父目录的修改时间
                updateModified(conn, parentStatus, ns.getModificationTime());
            }
            return new JdbcFSDataOutputStream(defaultBufferSize, uploadFile(ns));
        } else if (status.isDir()) {
            throw new FileAlreadyExistsException(f.toString());
        } else if (mode == WriteMode.NO_OVERWRITE) {
            throw new FileAlreadyExistsException(f.toString());
        } else {
            return new JdbcFSDataOutputStream(defaultBufferSize, uploadFile(status));
        }
    }

    @Override
    public boolean rename(final Path src, final Path dst) throws IOException {
        checkPath(src, dst);
        if (!src.isAbsolute()) {
            return rename(new Path(getWorkingDirectory(), src), dst);
        } else if (!dst.isAbsolute()) {
            return rename(src, new Path(getWorkingDirectory(), dst));
        }
        return connection(true, conn -> rename(conn, src, dst) > 0,
                "rename", src.toString(), dst.toString());
    }

    private int rename(final Connection conn, final Path src, final Path dst) throws IOException {
        final JdbcFileStatus srcStatus = getFileStatus(conn, src);
        final JdbcFileStatus dstStatus = getFileStatus(conn, dst);
        if (srcStatus == null) {
            throw new FileNotFoundException(src.toString());
        } else if (srcStatus.isDir()) {
            if (src.equals(dst)) {
                return 0;
            } else if (dstStatus != null && !dstStatus.isDir()) {
                throw new FileAlreadyExistsException(dst.toString());
            } else if (dst.getPath().startsWith(src.getPath() + Path.SEPARATOR)) {
                throw new FileSystemLoopException(dst.toString());
            } else {
                // move dir to dir
                final JdbcFileStatus dstDir = dstStatus == null ? mkdirs(conn, dst.getParent()) : dstStatus;
                final String dstName = (dstStatus != null || dst.getName().length() == 0 ? src : dst).getName();
                if (dstDir.getId() == srcStatus.getParent() && dstName.equals(src.getName())) {
                    return 0; // not change
                } else {
                    return move(conn, srcStatus, dstDir, dstName) + moveSub(conn, srcStatus, dstDir, dstName);
                }
            }
        } else if (dstStatus != null && !dstStatus.isDir()) {
            throw new FileAlreadyExistsException(dst.toString());
        } else {
            // move file to dir
            final JdbcFileStatus dstDir = dstStatus == null ? mkdirs(conn, dst.getParent()) : dstStatus;
            final String dstName = (dstStatus != null || dst.getName().length() == 0 ? src : dst).getName();
            if (dstDir.getId() == srcStatus.getParent() && dstName.equals(src.getName())) {
                return 0; // not change
            } else {
                return move(conn, srcStatus, dstDir, dstName);
            }
        }
    }

    @Override
    public boolean isDistributedFS() {
        return true;
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }

    private void checkPath(final Path... paths) throws IOException {
        checkNotNull(paths, "paths is null");
        int i = 0;
        for (Path f : paths) {
            checkNotNull(f, "paths[" + i + "] is null");
            if (!table.equals(f.toUri().getAuthority())) {
                throw new IOException("paths[" + i + "]'s table unknown: " + f.toUri().getAuthority());
            }
        }
    }

    private <R> R connection(final boolean transaction, final Function<Connection, R> function,
                             final String... title) throws IOException {
        final long start = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            if (!transaction) {
                return function.apply(conn);
            } else {
                conn.setAutoCommit(false);
                try {
                    final R res = function.apply(conn);
                    conn.commit();
                    return res;
                } catch (Throwable e) {
                    conn.rollback();
                    throw e;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            LOGGER.debug("connection[" + transaction + "] cost: " + (System.currentTimeMillis() - start)
                    + " " + Arrays.toString(title));
        }
    }


    private static <R> R statement(final Connection conn, final String sql, final boolean generatedKeys,
                                   final Function<PreparedStatement, R> function) throws IOException {
        try (PreparedStatement ps = conn.prepareStatement(sql,
                generatedKeys ? PreparedStatement.RETURN_GENERATED_KEYS : PreparedStatement.NO_GENERATED_KEYS)) {
            return function.apply(ps);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private JdbcFileStatus insert(final Connection conn, final Path f, final long parentId,
                                  final boolean isFile) throws IOException {
        return statement(conn, sqlInsert, true, ps -> {
            final long now = System.currentTimeMillis();
            int parameterIndex = 1;
            ps.setLong(parameterIndex++, parentId);
            ps.setString(parameterIndex++, f.getName());
            ps.setString(parameterIndex++, f.getPath());
            ps.setBoolean(parameterIndex++, isFile);
            ps.setLong(parameterIndex++, now);
            ps.setLong(parameterIndex, now);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    final long id = rs.getLong(1);
                    if (isFile) {
                        return JdbcFileStatus.file(this, id, parentId, f.getPath(), 0, now, now);
                    } else {
                        return JdbcFileStatus.dir(this, id, parentId, f.getPath(), now, now);
                    }
                }
                throw new FileNotFoundException(f.toString());
            }
        });
    }

    private void updateModified(final Connection conn, final JdbcFileStatus status,
                                final long modified) throws IOException {
        statement(conn, sqlUpdateModified, false, ps -> {
            ps.setLong(1, modified);
            ps.setLong(2, status.getId());
            return ps.executeUpdate() > 0;
        });
    }

    private int move(final Connection conn, final JdbcFileStatus src, final JdbcFileStatus dstDir,
                     final String dstName) throws IOException {
        final Path dst = new Path(dstDir.getPath(), dstName);
        final int count = statement(conn, sqlMove, false, ps -> {
            int parameterIndex = 1;
            ps.setLong(parameterIndex++, dstDir.getId());
            ps.setString(parameterIndex++, dstName);
            ps.setString(parameterIndex++, dst.getPath());
            ps.setLong(parameterIndex++, src.getId());
            ps.setLong(parameterIndex++, src.getParent());
            ps.setString(parameterIndex, src.getPath().getName());
            return ps.executeUpdate();
        });
        LOGGER.info("move[" + count + "] from: " + src.getPath() + " to: " + dst);
        return count;
    }

    private int moveSub(final Connection conn, final JdbcFileStatus src, final JdbcFileStatus dstDir,
                        final String dstName) throws IOException {
        final Path dst = new Path(dstDir.getPath(), dstName);
        final int count = statement(conn, sqlMoveSub, false, ps -> {
            int parameterIndex = 1;
            ps.setString(parameterIndex++, dst.getPath());
            ps.setInt(parameterIndex++, src.getPath().getPath().length() + 1);
            ps.setString(parameterIndex, src.getPath().getPath() + "/%");
            return ps.executeUpdate();
        });
        LOGGER.info("move sub[" + count + "] from: " + src.getPath() + " to: " + dst);
        return count;
    }

    private SizeConsumer<? super InputStream> uploadFile(final JdbcFileStatus status) {
        return (is, size) -> connection(true, conn -> statement(conn, sqlUpdateData, false, ps -> {
            final long now = System.currentTimeMillis();
            int parameterIndex = 1;
            ps.setBlob(parameterIndex++, is);
            ps.setLong(parameterIndex++, size);
            ps.setLong(parameterIndex++, now);
            ps.setLong(parameterIndex, status.getId());
            return ps.executeUpdate() > 0;
        }), "uploadFile", status.getPath().toString());
    }
}
