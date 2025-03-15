package com.github.macwille;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ParallelQueryList {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelQueryList.class);

    private final HikariDataSource hikariDataSource;
    private final List<String> queryList;
    private final int threads;

    public ParallelQueryList(final HikariDataSource hikariDataSource, final List<String> queryList) {
        this(hikariDataSource, queryList, 4);
    }

    public ParallelQueryList(final HikariDataSource hikariDataSource, final List<String> queryList, final int threads) {
        this.hikariDataSource = hikariDataSource;
        this.queryList = queryList;
        this.threads = threads;
    }

    public List<List<String>> resultList() throws SQLException {

        final Queue<ThreadedQuery> threadedQueries = new LinkedList<>();

        for (final String querySQL : queryList) {
            final ThreadedQuery threadedQuery = new ThreadedQuery(hikariDataSource, querySQL);
            threadedQueries.add(threadedQuery);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Executing <{}> queries using <{}> threads", queryList.size(), threads);
        }
        final List<List<String>> resultList = new ArrayList<>(threadedQueries.size());

        try (final ExecutorService executorService = Executors.newFixedThreadPool(4)) {

            while (!threadedQueries.isEmpty()) {
                final List<ThreadedQuery> batch = new ArrayList<>(threads);
                while (batch.size() < threads && !threadedQueries.isEmpty()) {
                    batch.add(threadedQueries.poll());
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Executing first batch size of <{}>", batch.size());
                }

                final List<Future<List<String>>> futureResults = executorService.invokeAll(batch);

                for (final Future<List<String>> future : futureResults) {
                    final List<String> result = future.get();
                    resultList.add(result);
                }

            }
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error executing threadded queries: " + e.getMessage(), e);
        }

        return resultList;
    }

}
