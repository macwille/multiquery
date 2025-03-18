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

import com.github.macwille.queries.CallableQuery;
import com.github.macwille.queries.ParallelQueryList;
import com.github.macwille.queries.QueriesFromStrings;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class QueriesToResults {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueriesToResults.class);

    private final DatasourceConfiguration configuration;
    private final List<String> queries;
    private final SQLDialect dialect;
    private final int threads;

    public QueriesToResults(final HikariConfig hikariConfig, final List<String> queries) {
        this(new DatasourceConfiguration(hikariConfig), queries, SQLDialect.MYSQL, 12);
    }

    public QueriesToResults(final DatasourceConfiguration configuration, final List<String> queries) {
        this(configuration, queries, SQLDialect.MYSQL, 12);
    }

    public QueriesToResults(
            final DatasourceConfiguration configuration,
            final List<String> queries,
            final SQLDialect dialect
    ) {
        this(configuration, queries, dialect, 12);
    }

    public QueriesToResults(
            final DatasourceConfiguration configuration,
            final List<String> queries,
            final SQLDialect dialect,
            int threads
    ) {
        this.configuration = configuration;
        this.queries = queries;
        this.dialect = dialect;
        this.threads = threads;
    }

    public List<Result<Record>> results() {
        LOGGER.trace("Executing Queries <{}>", queries);
        final HikariDataSource hikariDataSource = configuration.dataSource();
        final QueriesFromStrings queriesFromStrings = new QueriesFromStrings(hikariDataSource, queries, dialect);
        final List<CallableQuery> queryList = queriesFromStrings.queries();
        final ParallelQueryList parallelQueryList = new ParallelQueryList(queryList, threads);
        return parallelQueryList.resultList();
    }
}
