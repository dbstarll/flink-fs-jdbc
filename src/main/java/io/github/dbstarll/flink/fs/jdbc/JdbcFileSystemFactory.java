package io.github.dbstarll.flink.fs.jdbc;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

public class JdbcFileSystemFactory implements FileSystemFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcFileSystemFactory.class);

    private static final String SCHEME = "jdbc";
    private static final String CONFIG_PREFIX = "fs." + SCHEME + ".";
    private static final int CONFIG_PREFIX_LENGTH = CONFIG_PREFIX.length();
    private static final String DEFAULT_BUFFER_SIZE = "1024";

    private static volatile DataSource dataSource;

    private final Properties dataSourceProperties = new Properties();

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(final URI fsUri) throws IOException {
        LOGGER.info("create: " + fsUri);
        try {
            final String bufferSize = dataSourceProperties.getProperty("bufferSize", DEFAULT_BUFFER_SIZE);
            return new JdbcFileSystem(getDataSource(dataSourceProperties), Integer.parseInt(bufferSize), fsUri);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static DataSource getDataSource(final Properties dataSourceProperties) throws Exception {
        if (dataSource == null) {
            synchronized (JdbcFileSystemFactory.class) {
                if (dataSource == null) {
                    dataSource = DruidDataSourceFactory.createDataSource(dataSourceProperties);
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        try {
                            ((Closeable) dataSource).close();
                        } catch (IOException e) {
                            LOGGER.error("close dataSource failed.", e);
                        }
                    }));
                }
            }
        }
        return dataSource;
    }

    @Override
    public void configure(final Configuration config) {
        LOGGER.info("configure");
        dataSourceProperties.clear();
        for (final String key : config.keySet()) {
            if (key.startsWith(CONFIG_PREFIX)) {
                final String propKey = key.substring(CONFIG_PREFIX_LENGTH);
                final String propValue = config.getString(ConfigOptions.key(key).stringType().noDefaultValue());
                dataSourceProperties.put(propKey, propValue);
            }
        }
    }
}
