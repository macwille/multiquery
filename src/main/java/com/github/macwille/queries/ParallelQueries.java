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
import com.github.macwille.ThreadRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ParallelQueries {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelQueries.class);

    private final QueriesToBatches queryList;
    private final int threads;

    public ParallelQueries(final List<Query> queryList) {
        this(new QueriesToBatches(new LinkedList<>(queryList), 4), 4);
    }

    public ParallelQueries(final QueriesToBatches queryList, final int threads) {
        this.queryList = queryList;
        this.threads = threads;
    }

    public List<List<com.github.macwille.Record>> resultList() {

        final List<List<com.github.macwille.Record>> resultList = new ArrayList<>();

        try (final ExecutorService executorService = Executors.newFixedThreadPool(threads)) {

            while (queryList.hasNext()) {
                final List<Query> queryBatch = queryList.next();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Executing next batch with size <{}>", queryBatch.size());
                }

                final List<Future<List<com.github.macwille.Record>>> futureResults = executorService
                        .invokeAll(queryBatch);

                for (final Future<List<Record>> future : futureResults) {
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
