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

import org.jooq.SQLDialect;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public final class QueriesFromStrings {

    private final DataSource dataSource;
    private final List<String> sqlList;
    private final SQLDialect dialect;

    public QueriesFromStrings(final DataSource dataSource, final List<String> sqlList) {
        this(dataSource, sqlList, SQLDialect.MYSQL);
    }

    public QueriesFromStrings(final DataSource dataSource, final List<String> sqlList, SQLDialect dialect) {
        this.dataSource = dataSource;
        this.sqlList = sqlList;
        this.dialect = dialect;
    }

    public List<CallableQuery> queries() {

        final List<CallableQuery> callableQueryQueue = new ArrayList<>(sqlList.size());
        for (final String sql : sqlList) {
            callableQueryQueue.add(new QueryFromString(dataSource, sql, dialect));
        }
        return callableQueryQueue;
    }
}
