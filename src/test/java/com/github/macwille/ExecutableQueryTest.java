/*
 * MIT License
 *
 * Copyright (c) 2025 Ville Manninen
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.github.macwille;

import java.sql.*;
import java.util.List;
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
final class ExecutableQueryTest {

    private HikariDataSource hikariDataSource;
    private final String dbUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";

    @BeforeAll
    void setUp() {
        final Connection setupConn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(dbUrl));

        Assertions.assertDoesNotThrow(() -> {
            PreparedStatement stmt = setupConn
                    .prepareStatement("CREATE TABLE threaded_query_test (id INT PRIMARY KEY, name VARCHAR(255));");
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

        final String querySql = "SELECT * FROM threaded_query_test";
        final Query query = new ExecutableQuery(new HikariDataSource(hikariConfig), querySql);
        final List<String> result = Assertions.assertDoesNotThrow(query::call);
        Assertions.assertEquals(10000, result.size());
    }

    @Test
    void testParallelQueries() {
        final String firstQuerySql = "SELECT * FROM threaded_query_test WHERE id BETWEEN 1 AND 5000";
        final String secondQuerySql = "SELECT * FROM threaded_query_test WHERE id BETWEEN 5001 AND 10000";

        final Query firstQuery = new ExecutableQuery(hikariDataSource, firstQuerySql);
        final Query secondQuery = new ExecutableQuery(hikariDataSource, secondQuerySql);

        final ExecutorService executorService = Assertions.assertDoesNotThrow(() -> Executors.newFixedThreadPool(2));
        final Future<List<String>> future1 = executorService.submit(firstQuery);
        final Future<List<String>> future2 = executorService.submit(secondQuery);

        final long startTime = System.currentTimeMillis();
        while (!future1.isDone() || !future2.isDone()) {
            if (System.currentTimeMillis() - startTime > 10000) {
                break;
            }
            try { // limits cpu usage
                Thread.sleep(30);
            }
            catch (InterruptedException e) {
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
                stmt.execute("INSERT INTO threaded_query_test (id, name) VALUES (" + i + ", 'Name" + i + "');");
            }
        }
    }
}
