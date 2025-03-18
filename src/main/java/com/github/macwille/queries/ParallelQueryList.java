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
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ParallelQueryList {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelQueryList.class);

    private final QueriesToBatches queryList;
    private final int threads;

    public ParallelQueryList(final List<CallableQuery> queryList) {
        this(new QueriesToBatches(new LinkedList<>(queryList), 4), 4);
    }

    public ParallelQueryList(final List<CallableQuery> queryList, final int threads) {
        this(new QueriesToBatches(new LinkedList<>(queryList), threads), threads);
    }

    public ParallelQueryList(final QueriesToBatches queryList, final int threads) {
        this.queryList = queryList;
        this.threads = threads;
    }

    public List<Result<Record>> resultList() {

        final List<Result<Record>> resultList = new ArrayList<>();

        try (final ExecutorService executorService = Executors.newFixedThreadPool(threads)) {

            while (queryList.hasNext()) {
                final List<CallableQuery> queryBatches = queryList.next();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Executing next batch with size <{}>", queryBatches.size());
                }

                final List<Future<Result<Record>>> futureResults = executorService.invokeAll(queryBatches);

                for (final Future<Result<Record>> future : futureResults) {
                    resultList.add(future.get());
                }
            }
        }
        catch (final InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new ThreadRuntimeException(
                    "Thread encountered exception executing queries and was interrupted: " + e.getMessage(),
                    e
            );
        }

        return resultList;
    }

}
