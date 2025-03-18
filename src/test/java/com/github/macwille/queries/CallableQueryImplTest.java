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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

@TestInstance(Lifecycle.PER_CLASS)
final class CallableQueryImplTest {

    private HikariDataSource hikariDataSource;
    private final String url = "jdbc:h2:mem:testdb3;DB_CLOSE_DELAY=-1;MODE=MySQL";

    @BeforeAll
    void setUp() {
        final Connection setupConn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url));

        Assertions.assertDoesNotThrow(() -> {
            PreparedStatement stmt = setupConn
                    .prepareStatement("CREATE TABLE threaded_query_test (id INT PRIMARY KEY, name VARCHAR(255));");
            stmt.execute();
        });

        Assertions.assertDoesNotThrow(() -> {
            try (final Statement stmt = setupConn.createStatement()) {
                for (int i = 1; i <= 10000; i++) {
                    stmt.execute("INSERT INTO threaded_query_test (id, name) VALUES (" + i + ", 'Name" + i + "');");
                }
            }
        });

        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
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
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setMaximumPoolSize(2);

        final String querySql = "SELECT * FROM threaded_query_test";
        final CallableQuery query = new CallableQueryImpl(new HikariDataSource(hikariConfig), querySql);
        final List<Record> result = Assertions.assertDoesNotThrow(query::call);
        Assertions.assertEquals(10000, result.size());
    }
}
