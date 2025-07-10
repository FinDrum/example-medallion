package com.subjectstore.service;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import systems.intino.datamarts.subjectstore.SubjectStore;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SubjectStoreService {

    private final Map<String, StoreWithConnection> stores = new ConcurrentHashMap<>();
    private final DataSource dataSource;
    private final String basePath;

    public SubjectStoreService(DataSource dataSource, @Value("${subjectstore.index.base-path:data}") String basePath) {
        this.dataSource = dataSource;
        this.basePath = basePath;
    }

    public synchronized SubjectStore getOrCreateStore(String id, String type) {
        String name = id + ":" + type;

        return stores.compute(name, (key, existing) -> {
            if (existing != null) {
                try {
                    if (!existing.connection().isClosed() && existing.connection().isValid(2)) {
                        return existing;
                    } else {
                        closeQuietly(existing.connection());
                    }
                } catch (SQLException e) {
                    closeQuietly(existing.connection());
                }
            }

            try {
                File file = new File(basePath, name + ".oss");
                file.getParentFile().mkdirs();
                SubjectStore store = new SubjectStore(file);
                Connection conn = dataSource.getConnection();

                conn.setAutoCommit(false);
                store.connection(conn);
                System.out.println("✅ Connected to: " + conn.getMetaData().getURL());

                return new StoreWithConnection(store, conn);
            } catch (Exception e) {
                throw new RuntimeException("❌ Failed to create SubjectStore for " + name + ": " + e.getMessage(), e);
            }
        }).store();
    }

    @PreDestroy
    public void shutdown() {
        stores.values().forEach(entry -> {
            try {
                entry.store().seal();
                closeQuietly(entry.connection());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void closeQuietly(Connection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.rollback();
                conn.close();
            }
        } catch (Exception ignored) {
        }
    }

    private record StoreWithConnection(SubjectStore store, Connection connection) {}
}