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
package com.github.macwille.queries;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class ParallelQueryListTest {

    private final String url = "jdbc:h2:mem:testdb2;DB_CLOSE_DELAY=-1;MODE=MySQL";
    Connection setupconnection;

    @BeforeAll
    void setUp() throws Exception {
        setupconnection = DriverManager.getConnection(url);
        Assertions.assertDoesNotThrow(() -> {
            PreparedStatement stmt = setupconnection
                    .prepareStatement("CREATE TABLE parallel_query_test (id INT PRIMARY KEY, name VARCHAR(255));");
            stmt.execute();
        });
        Assertions.assertDoesNotThrow(() -> {
            try (final Statement stmt = setupconnection.createStatement()) {
                for (int i = 1; i <= 20000; i++) {
                    stmt.execute("INSERT INTO parallel_query_test (id, name) VALUES (" + i + ", 'Name" + i + "');");
                }
            }
        });
    }

    @AfterAll
    void tearDown() {
        Assertions.assertDoesNotThrow(setupconnection::close);
    }

    @Test
    void testFourParallelQueries() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setMaximumPoolSize(4);

        final HikariDataSource hikariDataSource = Assertions
                .assertDoesNotThrow(() -> new HikariDataSource(hikariConfig));

        final String query1Sql = "SELECT * FROM parallel_query_test WHERE id BETWEEN 1 AND 5000";
        final String query2Sql = "SELECT * FROM parallel_query_test WHERE id BETWEEN 5001 AND 10000";
        final String query3Sql = "SELECT * FROM parallel_query_test WHERE id BETWEEN 10001 AND 15000";
        final String query4Sql = "SELECT * FROM parallel_query_test WHERE id BETWEEN 15001 AND 20000";

        final QueriesFromStrings queriesFromStrings = new QueriesFromStrings(
                hikariDataSource,
                List.of(query1Sql, query2Sql, query3Sql, query4Sql)
        );
        final List<CallableQuery> queryList = queriesFromStrings.queries();

        final ParallelQueryList parallelQueryList = new ParallelQueryList(queryList);
        final List<Result<Record>> resultListList = Assertions.assertDoesNotThrow(parallelQueryList::resultList);

        int loops = 0;
        for (final Result<Record> resultList : resultListList) {
            Assertions.assertEquals(5000, resultList.size());
            loops++;
        }
        Assertions.assertEquals(4, loops);
    }

    @Test
    void testOverPoolLimit() {

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setMaximumPoolSize(2);

        final HikariDataSource hikariDataSource = Assertions
                .assertDoesNotThrow(() -> new HikariDataSource(hikariConfig));

        final String query1Sql = "SELECT * FROM parallel_query_test WHERE id BETWEEN 1 AND 5000";
        final String query2Sql = "SELECT * FROM parallel_query_test WHERE id BETWEEN 5001 AND 10000";
        final String query3Sql = "SELECT * FROM parallel_query_test WHERE id BETWEEN 10001 AND 15000";
        final String query4Sql = "SELECT * FROM parallel_query_test WHERE id BETWEEN 15001 AND 20000";

        final QueriesFromStrings queriesFromStrings = new QueriesFromStrings(
                hikariDataSource,
                List.of(query1Sql, query2Sql, query3Sql, query4Sql)
        );
        final List<CallableQuery> callableQueryList = queriesFromStrings.queries();

        final ParallelQueryList parallelQueryList = new ParallelQueryList(callableQueryList);
        final List<Result<Record>> resultListList = Assertions.assertDoesNotThrow(parallelQueryList::resultList);

        int loops = 0;
        for (final Result<Record> resultList : resultListList) {
            Assertions.assertEquals(5000, resultList.size());
            loops++;
        }
        Assertions.assertEquals(4, loops);
    }
}
