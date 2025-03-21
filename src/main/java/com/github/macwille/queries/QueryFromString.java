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

import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;

import javax.sql.DataSource;

public final class QueryFromString implements CallableQuery {

    private final CallableQuery query;

    public QueryFromString(final DataSource dataSource, final String sql) {
        this(new CallableQueryImpl(dataSource, sql, SQLDialect.MYSQL));
    }

    public QueryFromString(final DataSource dataSource, final String sql, final SQLDialect dialect) {
        this(new CallableQueryImpl(dataSource, sql, dialect));
    }

    public QueryFromString(final CallableQuery query) {
        this.query = query;
    }

    @Override
    public Result<Record> call() throws Exception {
        return query.call();
    }
}
