package io.github.dbstarll.flink.fs.jdbc;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.commons.io.IOUtils;
import org.apache.flink.core.fs.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.sql.Connection;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class JdbcFileSystemTest {
    private volatile DataSource ds;
    private volatile FileSystem fs;

    @BeforeEach
    void setUp() throws Exception {
        final Properties dataSourceProperties = new Properties();
        dataSourceProperties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("jdbc.properties"));
        this.ds = DruidDataSourceFactory.createDataSource(dataSourceProperties);
        this.fs = new JdbcFileSystem(ds, 1024, URI.create("jdbc://test/default"));
        this.ds.getConnection().close();
    }

    @AfterEach
    void tearDown() throws Exception {
        this.fs = null;
        if (this.ds instanceof Closeable) {
            try (Connection conn = ds.getConnection()) {
                conn.createStatement().executeUpdate("truncate table test");
            }
            ((Closeable) this.ds).close();
        }
        this.ds = null;
    }

    @Test
    void mkdirs() throws IOException {
        final Path path = new Path(URI.create("jdbc://test/default/blob"));

        try {
            fs.getFileStatus(path);
            fail("must throws FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertEquals(path.toString(), e.getMessage());
        }

        assertTrue(fs.mkdirs(path));

        final FileStatus status = fs.getFileStatus(path);
        assertNotNull(status);
        assertTrue(status.isDir());
        assertEquals(path, status.getPath());
        //检查父目录的更新时间
        assertEquals(status.getModificationTime(), fs.getFileStatus(path.getParent()).getModificationTime());

        FileStatus[] statuses = fs.listStatus(path.getParent());
        assertNotNull(statuses);
        assertEquals(1, statuses.length);
        assertTrue(statuses[0].isDir());
        assertEquals(path, statuses[0].getPath());

        statuses = fs.listStatus(path);
        assertNotNull(statuses);
        assertEquals(0, statuses.length);
    }

    @Test
    void create() throws IOException {
        final Path path = new Path(URI.create("jdbc://test/default/blob/abc"));
        final String content = UUID.randomUUID().toString();

        try {
            fs.open(path);
            fail("must throws FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertEquals(path.toString(), e.getMessage());
        }

        try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
        final FileStatus[] statuses = fs.listStatus(path.getParent());
        assertNotNull(statuses);
        assertEquals(1, statuses.length);
        assertFalse(statuses[0].isDir());
        assertEquals(path, statuses[0].getPath());
        assertEquals(36, statuses[0].getLen());

        final FSDataInputStream is = fs.open(path);
        final byte[] data = IOUtils.readFully(is, is.available());
        assertEquals(36, data.length);
        assertEquals(content, new String(data, StandardCharsets.UTF_8));

        try {
            fs.open(path.getParent());
            fail("must throws FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertEquals(path.getParent().toString(), e.getMessage());
        }
    }

    @Test
    void delete() throws IOException {
        final Path path = new Path(URI.create("jdbc://test/default/blob/abc"));
        final String content = UUID.randomUUID().toString();

        try {
            fs.delete(path, false);
            fail("must throws FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertEquals(path.toString(), e.getMessage());
        }

        try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }

        try {
            fs.delete(path.getParent(), false);
            fail("must throws DirectoryNotEmptyException");
        } catch (DirectoryNotEmptyException e) {
            assertEquals(path.getParent().toString(), e.getMessage());
        }

        assertTrue(fs.delete(path, false));
        assertTrue(fs.delete(path.getParent(), false));
        assertEquals(0, fs.listStatus(path.getParent().getParent()).length);
    }

    @Test
    void deleteRecursive() throws IOException {
        final Path path = new Path(URI.create("jdbc://test/default/blob/abc"));
        final String content = UUID.randomUUID().toString();

        try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }

        assertTrue(fs.delete(path.getParent().getParent(), true));

        assertEquals(0, fs.listStatus(path.getParent().getParent().getParent()).length);
    }

    @Test
    void rename() throws IOException {
        final Path src = new Path(URI.create("jdbc://test/default/blob/abc"));
        final String content = UUID.randomUUID().toString();

        try (FSDataOutputStream out = fs.create(src, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }

        final Path dstRenameToSameDir = new Path(URI.create("jdbc://test/default/blob/def"));
        assertTrue(fs.rename(src, dstRenameToSameDir));
        FileStatus[] statuses = fs.listStatus(src.getParent());
        assertEquals(1, statuses.length);
        assertEquals(dstRenameToSameDir, statuses[0].getPath());

        final Path dstRenameToAnotherDir = new Path(URI.create("jdbc://test/default/blob2/ghi"));
        assertTrue(fs.rename(dstRenameToSameDir, dstRenameToAnotherDir));
        assertEquals(0, fs.listStatus(src.getParent()).length);
        statuses = fs.listStatus(dstRenameToAnotherDir.getParent());
        assertEquals(1, statuses.length);
        assertEquals(dstRenameToAnotherDir, statuses[0].getPath());

        final Path dstMoveToAnotherDir = new Path(URI.create("jdbc://test/default/blob3/"));
        assertTrue(fs.rename(dstRenameToAnotherDir, dstMoveToAnotherDir));
        assertEquals(0, fs.listStatus(dstRenameToAnotherDir.getParent()).length);
        statuses = fs.listStatus(dstMoveToAnotherDir.getParent());
        assertEquals(1, statuses.length);
        assertEquals(new Path(dstMoveToAnotherDir, dstRenameToAnotherDir.getName()), statuses[0].getPath());

        final Path dstMoveDirToAnotherDir = new Path(URI.create("jdbc://test/default/blob4/"));
        assertTrue(fs.rename(dstMoveToAnotherDir.getParent(), dstMoveDirToAnotherDir));
        final Path dstMoveDirToAnotherDirFinal = new Path("jdbc://test/default/blob4/blob3");

        statuses = fs.listStatus(dstMoveDirToAnotherDir.getParent());
        assertEquals(1, statuses.length);
        assertEquals(dstMoveDirToAnotherDirFinal, statuses[0].getPath());

        statuses = fs.listStatus(dstMoveDirToAnotherDirFinal);
        assertEquals(1, statuses.length);
        assertEquals("jdbc://test/default/blob4/blob3/ghi", statuses[0].getPath().toString());

        final Path dstMoveDirToExist = new Path("jdbc://test/default");
        assertTrue(fs.rename(dstMoveDirToAnotherDirFinal, dstMoveDirToExist));
        for (FileStatus status : fs.listStatus(dstMoveDirToExist)) {
            System.out.println(status);
        }
        for (FileStatus status : fs.listStatus(new Path("jdbc://test/default/blob3"))) {
            System.out.println(status);
        }
    }
}