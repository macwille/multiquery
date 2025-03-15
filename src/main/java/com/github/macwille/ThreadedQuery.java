package com.github.macwille;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedQuery implements Callable<ResultSet> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadedQuery.class);

    private final Connection connection;
    private final String query;

    public ThreadedQuery(final Connection connection, final String query) {
        this.connection = connection;
        this.query = query;
    }

    @Override
    public ResultSet call() throws Exception {
        LOGGER.debug("Executing query: {}", query);
        final ResultSet resultSet;
        try (final PreparedStatement statement = connection.prepareStatement(query)) {
            resultSet = statement.executeQuery();
        }
        return resultSet;
    }
}
