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

import com.github.macwille.ThreadRuntimeException;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;

import javax.sql.DataSource;

public final class CallableQueryImpl implements CallableQuery {

    private final DSLContext ctx;
    private final String query;

    public CallableQueryImpl(final DataSource dataSource, final String query) {
        this(DSL.using(new DefaultConfiguration().derive(dataSource).derive(SQLDialect.MYSQL)), query);
    }

    public CallableQueryImpl(final DataSource dataSource, final String query, SQLDialect dialect) {
        this(DSL.using(new DefaultConfiguration().derive(dataSource).derive(dialect)), query);
    }

    public CallableQueryImpl(final DSLContext ctx, final String query) {
        this.ctx = ctx;
        this.query = query;
    }

    @Override
    public Result<org.jooq.Record> call() {
        Result<org.jooq.Record> result;
        try {
            result = ctx.fetch(query);
        } catch (DataAccessException e) {
            Thread.currentThread().interrupt();
            throw new ThreadRuntimeException("Error fetching record", e);
        }

        return result;
    }
}
