package com.github.macwille;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final public class ParallelQueryListTest {


    private final String url = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private final String username = "sa";
    private final String password = "";
    Connection setupconnection;

    @BeforeAll
    void setUp() throws Exception {
        setupconnection = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        Assertions.assertDoesNotThrow(() -> {
            PreparedStatement stmt = setupconnection
                    .prepareStatement("CREATE TABLE target_table (id INT PRIMARY KEY, name VARCHAR(255));");
            stmt.execute();
        });
        Assertions.assertDoesNotThrow(() -> generateRows(20000));
    }

    @AfterAll
    public void tearDown() {
        Assertions.assertDoesNotThrow(setupconnection::close);
    }

    @Test
    public void testFourParallelQueries() {
        final String query1 = "SELECT * FROM target_table WHERE id BETWEEN 1 AND 5000";
        final String query2 = "SELECT * FROM target_table WHERE id BETWEEN 5001 AND 10000";
        final String query3 = "SELECT * FROM target_table WHERE id BETWEEN 10001 AND 15000";
        final String query4 = "SELECT * FROM target_table WHERE id BETWEEN 15001 AND 20000";

        final List<String> queryList = List.of(query1, query2, query3, query4);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(username);
        hikariConfig.setUsername(password);
        hikariConfig.setMaximumPoolSize(4);

        final HikariDataSource hikariDataSource = Assertions.assertDoesNotThrow(() -> new HikariDataSource(hikariConfig));
        final ParallelQueryList parallelQueryList = new ParallelQueryList(hikariDataSource, queryList);
        final List<List<String>> resultListList = Assertions.assertDoesNotThrow(parallelQueryList::resultList);

        int loops = 0;
        for(final List<String> resultList: resultListList) {
            Assertions.assertEquals(5000, resultList.size());
            loops++;
        }
        Assertions.assertEquals(4, loops);
    }

    @Test
    public void testOverPoolLimit() {

        final String query1 = "SELECT * FROM target_table WHERE id BETWEEN 1 AND 5000";
        final String query2 = "SELECT * FROM target_table WHERE id BETWEEN 5001 AND 10000";
        final String query3 = "SELECT * FROM target_table WHERE id BETWEEN 10001 AND 15000";
        final String query4 = "SELECT * FROM target_table WHERE id BETWEEN 15001 AND 20000";

        final List<String> queryList = List.of(query1, query2, query3, query4);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(username);
        hikariConfig.setUsername(password);
        hikariConfig.setMaximumPoolSize(2);

        final HikariDataSource hikariDataSource = Assertions.assertDoesNotThrow(() -> new HikariDataSource(hikariConfig));
        final ParallelQueryList parallelQueryList = new ParallelQueryList(hikariDataSource, queryList);
        final List<List<String>> resultListList = Assertions.assertDoesNotThrow(parallelQueryList::resultList);

        int loops = 0;
        for(final List<String> resultList: resultListList) {
            Assertions.assertEquals(5000, resultList.size());
            loops++;
        }
        Assertions.assertEquals(4, loops);
    }

    private void generateRows(final int rowCount) throws Exception {
        try (final Statement stmt = setupconnection.createStatement()) {
            for (int i = 1; i <= rowCount; i++) {
                stmt.execute("INSERT INTO target_table (id, name) VALUES (" + i + ", 'Name" + i + "');");
            }
        }
    }
}
