package com.github.macwille;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
class ThreadedQueryTest {

  private Connection connection1;
  private Connection connection2;

  @BeforeAll
  void setUp() throws Exception {
    connection1 = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
    connection2 = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
    Assertions.assertDoesNotThrow(() -> {
      PreparedStatement stmt = connection1
          .prepareStatement("CREATE TABLE target_table (id INT PRIMARY KEY, name VARCHAR(255));");
      stmt.execute();
    });
    Assertions.assertDoesNotThrow(() -> generateRows(10000));
  }

  @AfterAll
  void tearDown() throws Exception {
    Assertions.assertDoesNotThrow(connection1::close);
    Assertions.assertDoesNotThrow(connection2::close);
  }

  @Test
  void testSingleQuery() {
    final String query = "SELECT * FROM target_table";
    final Callable<List<String>> threadedQuery = new ThreadedQuery(connection1, query);
    final List<String> result = Assertions.assertDoesNotThrow(threadedQuery::call);
    Assertions.assertEquals(10000, result.size());
  }

  @Test
  void testParallelQueries() {
    final String query1 = "SELECT * FROM target_table WHERE id BETWEEN 1 AND 5000";
    final Callable<List<String>> threadedQuery1 = new ThreadedQuery(connection1, query1);
    final String query2 = "SELECT * FROM target_table WHERE id BETWEEN 5001 AND 10000";
    final Callable<List<String>> threadedQuery2 = new ThreadedQuery(connection2, query2);
    final ExecutorService executorService = Executors.newFixedThreadPool(2);
    final Future<List<String>> future1 = executorService.submit(threadedQuery1);
    final Future<List<String>> future2 = executorService.submit(threadedQuery2);

    final long startTime = System.currentTimeMillis();
    while (!future1.isDone() || !future2.isDone()) {
      if (System.currentTimeMillis() - startTime > 10000) {
        break;
      }
      try {
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

  private void generateRows(int rowCount) throws Exception {
    try (Statement stmt = connection1.createStatement()) {
      for (int i = 1; i <= rowCount; i++) {
        stmt.execute("INSERT INTO target_table (id, name) VALUES (" + i + ", 'Name" + i + "');");
      }
    }
  }
}