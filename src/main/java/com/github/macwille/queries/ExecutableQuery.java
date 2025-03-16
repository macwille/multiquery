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

import com.github.macwille.Record;
import com.github.macwille.SQLRecord;
import com.github.macwille.ThreadRuntimeException;
import com.github.macwille.fields.*;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public final class ExecutableQuery implements Query {

    private final DataSource dataSource;
    private final String query;

    public ExecutableQuery(final DataSource dataSource, final String query) {
        this.dataSource = dataSource;
        this.query = query;
    }

    public List<com.github.macwille.Record> call() {
        final List<Record> results = new ArrayList<>();
        try (
                final Connection connection = dataSource.getConnection(); final PreparedStatement statement = connection
                .prepareStatement(query);
                final ResultSet resultSet = statement.executeQuery()
        ) {

            final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            final int resultSetColumnCount = resultSetMetaData.getColumnCount();

            while (resultSet.next()) {

                final List<Field<?>> rowFields = new ArrayList<>(resultSetColumnCount);

                for (int i = 1; i <= resultSetColumnCount; i++) {
                    final String fieldName = resultSetMetaData.getColumnName(i);
                    final String fieldType = resultSetMetaData.getColumnTypeName(i);
                    final Field<?> field = extractFieldValue(resultSet, i, fieldName, fieldType);
                    rowFields.add(field);
                }

                results.add(new SQLRecord(rowFields));
            }
        } catch (final UnsupportedFieldType | SQLException e) {
            Thread.currentThread().interrupt();
            throw new ThreadRuntimeException("Query execution failed", e);
        }
        return results;
    }

    private Field<?> extractFieldValue(final ResultSet resultSet, final int columnIndex, final String columnName, final String fieldType) throws UnsupportedFieldType, SQLException {

        final Field<?> field;

        if (resultSet.wasNull()) {
            field = new NullField(columnName);
        } else {
            final FieldFromValue<?> fieldFromValue = switch (fieldType.toUpperCase()) {
                case "VARCHAR", "CHAR", "CHARACTER VARYING", "TEXT", "TINYTEXT", "MEDIUMTEXT", "CLOB", "UUID", "JSON",
                     "JSONB", "ENUM ", "SET" ->
                        new FieldFromString(columnName, fieldType, resultSet.getString(columnIndex));
                case "INTEGER", "INT", "SMALLINT", "TINYINT", "TINYINT UNSIGNED", "SMALLINT UNSIGNED",
                     "MEDIUMINT UNSIGNED", "INT UNSIGNED", "INTEGER UNSIGNED", "BIGINT UNSIGNED" ->
                        new FieldFromInteger(columnName, fieldType, resultSet.getInt(columnIndex));
                case "BIGINT", "MONEY" -> new FieldFromLong(columnName, fieldType, resultSet.getLong(columnIndex));
                case "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT" ->
                        new FieldFromDouble(columnName, fieldType, resultSet.getDouble(columnIndex));
                case "DATE" -> new FieldFromDate(columnName, fieldType, resultSet.getDate(columnIndex));
                case "TIME" -> new FieldFromTime(columnName, resultSet.getTime(columnIndex));
                case "TIMESTAMP", "DATETIME" ->
                        new FieldFromTimestamp(columnName, fieldType, resultSet.getTimestamp(columnIndex));
                case "BOOLEAN", "BOOL" ->
                        new FieldFromBoolean(columnName, fieldType, resultSet.getBoolean(columnIndex));
                case "BLOB", "VARBINARY", "MEDIUMBLOB", "LONGTEXT", "BYTEA", "BINARY" ->
                        new FieldFromBytes(columnName, fieldType, resultSet.getBytes(columnIndex));
                default -> throw new UnsupportedFieldType(fieldType);
            };
            field = fieldFromValue.field();
        }
        return field;
    }
}
