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
package com.github.macwille.fields;

import org.h2.jdbc.JdbcBlob;
import org.junit.jupiter.api.*;

import java.sql.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResultFieldTest {

    private final String url = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    Connection conn;

    @BeforeAll
    void setup() {
        conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url));
        final String createTableSQL = "CREATE TABLE result_field_test (" + "id INT PRIMARY KEY, "
                + "varchar_col VARCHAR(255), " + "int_col INT, " + "smallint_col SMALLINT, " + "bigint_col BIGINT, "
                + "decimal_col DECIMAL(10,2), " + "double_col DOUBLE, " + "boolean_col BOOLEAN, " + "date_col DATE, "
                + "time_col TIME, " + "timestamp_col TIMESTAMP, " + "blob_col BLOB, " + "binary_col BINARY(5)" + ");";

        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(createTableSQL);
            stmt.execute();
        });

        final String insertSQL = "INSERT INTO result_field_test (id, varchar_col, int_col, smallint_col, bigint_col, decimal_col, double_col, boolean_col, date_col, time_col, timestamp_col, blob_col, binary_col) VALUES "
                + "(1, 'Test Name', 123, 32767, 9223372036854775807, 12345.67, 3.14159, TRUE, '2025-03-16', '12:30:00', '2025-03-16 12:30:00', ?, ?);";

        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(insertSQL);
            // Blob and Binary data
            stmt.setBytes(1, "binarydata".getBytes());
            stmt.setBytes(2, new byte[]{
                    0x01, 0x02, 0x03, 0x04, 0x05
            });

            stmt.execute();
        });

    }

    @AfterAll
    void tearDown() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    void testStringField() {
        final String sql = "SELECT varchar_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<String> field = new ResultField<>(
                        "varchar_col",
                        "VARCHAR",
                        resultSet.getString("varchar_col")
                );
                Assertions.assertEquals("varchar_col", field.name());
                Assertions.assertEquals("VARCHAR", field.type());
                Assertions.assertEquals("Test Name", field.value());
                Assertions.assertEquals(String.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testIntField() {
        final String sql = "SELECT int_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Integer> field = new ResultField<>("int_col", "INT", resultSet.getInt("int_col"));
                Assertions.assertEquals("int_col", field.name());
                Assertions.assertEquals("INT", field.type());
                Assertions.assertEquals(123, field.value());
                Assertions.assertEquals(Integer.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testSmallIntField() {
        final String sql = "SELECT smallint_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Integer> field = new ResultField<>(
                        "smallint_col",
                        "SMALLINT",
                        resultSet.getInt("smallint_col")
                );
                Assertions.assertEquals("smallint_col", field.name());
                Assertions.assertEquals("SMALLINT", field.type());
                Assertions.assertEquals(32767, field.value());
                Assertions.assertEquals(Integer.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testBigIntCol() {
        final String sql = "SELECT bigint_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Long> field = new ResultField<>("bigint_col", "BIGINT", resultSet.getLong("bigint_col"));
                Assertions.assertEquals("bigint_col", field.name());
                Assertions.assertEquals("BIGINT", field.type());
                Assertions.assertEquals(9223372036854775807L, field.value());
                Assertions.assertEquals(Long.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testDecimalCol() {
        final String sql = "SELECT decimal_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Double> field = new ResultField<>(
                        "decimal_col",
                        "DECIMAL(10,2)",
                        resultSet.getDouble("decimal_col")
                );
                Assertions.assertEquals("decimal_col", field.name());
                Assertions.assertEquals("DECIMAL(10,2)", field.type());
                Assertions.assertEquals(12345.67, field.value());
                Assertions.assertEquals(Double.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testDoubleCol() {
        final String sql = "SELECT double_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Double> field = new ResultField<>(
                        "double_col",
                        "DOUBLE",
                        resultSet.getDouble("double_col")
                );
                Assertions.assertEquals("double_col", field.name());
                Assertions.assertEquals("DOUBLE", field.type());
                Assertions.assertEquals(3.14159, field.value());
                Assertions.assertEquals(Double.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testBooleanCol() {
        final String sql = "SELECT boolean_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Boolean> field = new ResultField<>(
                        "boolean_col",
                        "BOOLEAN",
                        resultSet.getBoolean("boolean_col")
                );
                Assertions.assertEquals("boolean_col", field.name());
                Assertions.assertEquals("BOOLEAN", field.type());
                Assertions.assertEquals(true, field.value());
                Assertions.assertEquals(Boolean.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testDateCol() {
        final String sql = "SELECT date_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Date> field = new ResultField<>("date_col", "BOOLEAN", resultSet.getDate("date_col"));
                Assertions.assertEquals("date_col", field.name());
                Assertions.assertEquals("BOOLEAN", field.type());
                Assertions.assertEquals(Date.valueOf("2025-03-16"), field.value());
                Assertions.assertEquals(Date.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testTimeCol() {
        final String sql = "SELECT time_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Time> field = new ResultField<>("time_col", "TIME", resultSet.getTime("time_col"));
                Assertions.assertEquals("time_col", field.name());
                Assertions.assertEquals("TIME", field.type());
                Assertions.assertEquals(Time.valueOf("12:30:00"), field.value());
                Assertions.assertEquals(Time.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testTimeStampCol() {
        final String sql = "SELECT timestamp_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Timestamp> field = new ResultField<>(
                        "timestamp_col",
                        "TIMESTAMP",
                        resultSet.getTimestamp("timestamp_col")
                );
                Assertions.assertEquals("timestamp_col", field.name());
                Assertions.assertEquals("TIMESTAMP", field.type());
                Assertions.assertEquals(Timestamp.valueOf("2025-03-16 12:30:00"), field.value());
                Assertions.assertEquals(Timestamp.class, field.value().getClass());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testBinaryLargeObjectCol() {
        final String sql = "SELECT blob_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<Blob> field = new ResultField<>("blob_col", "BLOB", resultSet.getBlob("blob_col"));
                Assertions.assertEquals("blob_col", field.name());
                Assertions.assertEquals("BLOB", field.type());
                Assertions
                        .assertArrayEquals(
                                "binarydata".getBytes(), field.value().getBytes(0, (int) field.value().length())
                        );
                Assertions.assertEquals(JdbcBlob.class, field.value().getClass());
                Assertions.assertTrue(java.sql.Blob.class.isAssignableFrom(field.value().getClass()));
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    void testBinaryCol() {
        final String sql = "SELECT binary_col FROM result_field_test WHERE id = 1";
        Assertions.assertDoesNotThrow(() -> {
            final PreparedStatement stmt = conn.prepareStatement(sql);
            final ResultSet resultSet = stmt.executeQuery();
            int loops = 0;
            if (resultSet.next()) {
                final Field<byte[]> field = new ResultField<>("binary_col", "BINARY", resultSet.getBytes("binary_col"));
                Assertions.assertEquals("binary_col", field.name());
                Assertions.assertEquals("BINARY", field.type());
                Assertions.assertArrayEquals(new byte[]{
                        0x01, 0x02, 0x03, 0x04, 0x05
                }, field.value());
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }
}
