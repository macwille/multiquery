package com.github.macwille;

import java.sql.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
final class ThreadedQueryTest {
  private HikariDataSource hikariDataSource;
  private final String dbUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";

  @BeforeAll
  void setUp() {
    final Connection setupConn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(dbUrl));

    Assertions.assertDoesNotThrow(() -> {
      PreparedStatement stmt = setupConn
          .prepareStatement("CREATE TABLE target_table (id INT PRIMARY KEY, name VARCHAR(255));");
      stmt.execute();
    });

    Assertions.assertDoesNotThrow(() -> generateRows(setupConn, 10000));

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(dbUrl);
    hikariConfig.setUsername("sa");
    hikariConfig.setUsername("");
    hikariConfig.setMaximumPoolSize(2);
    hikariDataSource = new HikariDataSource(hikariConfig);
  }

  @AfterAll
  void tearDown() {
    Assertions.assertDoesNotThrow(() -> hikariDataSource.close());
  }

  @Test
  void testSingleQuery() {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
    hikariConfig.setUsername("sa");
    hikariConfig.setUsername("");
    hikariConfig.setMaximumPoolSize(2);

    final String query = "SELECT * FROM target_table";
    final Callable<List<String>> threadedQuery = new ThreadedQuery(new HikariDataSource(hikariConfig), query);
    final List<String> result = Assertions.assertDoesNotThrow(threadedQuery::call);
    Assertions.assertEquals(10000, result.size());
  }

  @Test
  void testParallelQueries() {
    final String query1 = "SELECT * FROM target_table WHERE id BETWEEN 1 AND 5000";
    final Callable<List<String>> threadedQuery1 = new ThreadedQuery(hikariDataSource, query1);
    final String query2 = "SELECT * FROM target_table WHERE id BETWEEN 5001 AND 10000";
    final Callable<List<String>> threadedQuery2 = new ThreadedQuery(hikariDataSource, query2);
    final ExecutorService executorService = Executors.newFixedThreadPool(2);
    final Future<List<String>> future1 = executorService.submit(threadedQuery1);
    final Future<List<String>> future2 = executorService.submit(threadedQuery2);

    final long startTime = System.currentTimeMillis();
    while (!future1.isDone() || !future2.isDone()) {
      if (System.currentTimeMillis() - startTime > 10000) {
        break;
      }
      try { // limits cpu usage
        Thread.sleep(30);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    executorService.shutdown();
    Assertions.assertTrue(future1.isDone());
    Assertions.assertTrue(future2.isDone());
    List<String> result1 = Assertions.assertDoesNotThrow(() -> future1.get());
    List<String> result2 = Assertions.assertDoesNotThrow(() -> future2.get());

    Assertions.assertEquals(5000, result1.size());
    Assertions.assertEquals(5000, result2.size());

  }

  private void generateRows(final Connection connection, final int rowCount) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      for (int i = 1; i <= rowCount; i++) {
        stmt.execute("INSERT INTO target_table (id, name) VALUES (" + i + ", 'Name" + i + "');");
      }
    }
  }
}