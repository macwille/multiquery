package com.github.macwille;

import java.sql.ResultSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ThreadedQueryTest {

  @Test
  void testThreaddedQuery() {
    final String query = "SELECT * FROM target_table";
    final ThreadedQuery threadedQuery = new ThreadedQuery(null, query);
    final ResultSet result = Assertions.assertDoesNotThrow(threadedQuery::call);

    Assertions.assertDoesNotThrow(() -> {
      int loops = 0;
      while (result.next()) {
        loops++;
      }
      Assertions.assertEquals(0, loops);
    });
  }
}