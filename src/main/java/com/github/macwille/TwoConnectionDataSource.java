package com.github.macwille;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class TwoConnectionDataSource implements DataSource, AutoCloseable {
    private final List<Connection> connections;
    private final AtomicInteger index;

    public TwoConnectionDataSource(List<Connection> connections) {
        this(connections, new AtomicInteger(0));
    }

    private TwoConnectionDataSource(List<Connection> connections, AtomicInteger index) {
        this.connections = connections;
        this.index = index;
    }

    @Override
    public Connection getConnection() {
        int selectedIndex = index.getAndUpdate(i -> (i + 1) % 2);
        return connections.get(selectedIndex);
    }

    @Override
    public Connection getConnection(String username, String password) {
        return getConnection();
    }

    @Override
    public void close() throws SQLException {
        for (Connection conn : connections) {
            conn.close();
        }
    }

    @Override
    public PrintWriter getLogWriter() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int getLoginTimeout() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void setLoginTimeout(int seconds) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Logger getParentLogger() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        throw new UnsupportedOperationException("Not supported");
    }

}
