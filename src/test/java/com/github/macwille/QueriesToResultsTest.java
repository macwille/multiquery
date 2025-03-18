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

import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueriesToResultsTest {

    private final String url = "jdbc:h2:mem:testdb1;DB_CLOSE_DELAY=-1;USER=test;PASSWORD=test_password;MODE=MySQL";

    @BeforeAll
    void setUp() {
        final Connection setupConn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url));
        Assertions.assertDoesNotThrow(() -> {
            PreparedStatement stmt = setupConn
                    .prepareStatement("CREATE TABLE queries_to_results_test (id INT PRIMARY KEY, name VARCHAR(255));");
            stmt.execute();
        });

        Assertions.assertDoesNotThrow(() -> {
            try (final Statement stmt = setupConn.createStatement()) {
                for (int i = 1; i <= 100000; i++) {
                    stmt.execute("INSERT INTO queries_to_results_test (id, name) VALUES (" + i + ", 'Name" + i + "');");
                }
            }
        });
        Assertions.assertDoesNotThrow(setupConn::close);
    }

    @Test
    void testListOfQueries() {
        final Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("url", url);
        optionsMap.put("username", "test");
        optionsMap.put("password", "test_password");

        List<String> queries = List
                .of(
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 1 AND 5000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 5001 AND 10000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 10001 AND 15000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 15001 AND 20000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 20001 AND 25000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 25001 AND 30000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 30001 AND 35000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 35001 AND 40000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 40001 AND 45000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 45001 AND 50000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 50001 AND 55000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 55001 AND 60000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 60001 AND 65000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 65001 AND 70000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 70001 AND 75000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 75001 AND 80000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 80001 AND 85000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 85001 AND 90000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 90001 AND 95000",
                        "SELECT * FROM queries_to_results_test WHERE id BETWEEN 95001 AND 100000"
                );

        final DatasourceConfiguration datasourceConfiguration = new DatasourceConfiguration(optionsMap);
        final QueriesToResults queriesToResults = new QueriesToResults(
                datasourceConfiguration,
                queries,
                SQLDialect.MYSQL
        );
        final List<Result<Record>> results = queriesToResults.results();

        Assertions.assertEquals(20, results.size());

        int loops = 0;
        for (final Result<Record> result : results) {
            Assertions.assertEquals(5000, result.size());
            int innerLoops = 0;
            for (final Record record : result) {
                final Object id = record.get("ID");
                final Object name = record.get("NAME");
                Assertions.assertNotNull(id);
                Assertions.assertNotNull(name);
                Assertions.assertEquals("Name" + id, name.toString());
                innerLoops++;
            }
            Assertions.assertEquals(5000, innerLoops);
            loops++;
        }
        Assertions.assertEquals(20, loops);
    }
}
