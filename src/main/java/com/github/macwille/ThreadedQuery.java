package com.github.macwille;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedQuery implements Callable<List<String>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadedQuery.class);

  private final Connection connection;
  private final String query;

  public ThreadedQuery(final Connection connection, final String query) {
    this.connection = connection;
    this.query = query;
  }

  @Override
  public List<String> call() throws RuntimeException {
    LOGGER.debug("Executing query: {}", query);

    final List<String> results = new ArrayList<>();
    try (final PreparedStatement statement = connection.prepareStatement(query)) {
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        results.add(resultSet.getString(1));
      }
      
    } catch (final SQLException e) {
      throw new RuntimeException("Error processing query results: " + e.getMessage(), e);
    }

    return results;
  }
}
