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

import java.util.*;

public final class QueriesToBatches implements Iterator<List<CallableQuery>> {

    private final Queue<CallableQuery> queries;
    private final int batchSize;

    public QueriesToBatches(final Queue<CallableQuery> queries, final int batchSize) {
        this.queries = queries;
        this.batchSize = batchSize;
    }

    @Override
    public List<CallableQuery> next() throws NoSuchElementException {
        final List<CallableQuery> batch = new ArrayList<>(batchSize);
        while (batch.size() < batchSize && !queries.isEmpty()) {
            batch.add(queries.poll());
        }
        return batch;
    }

    @Override
    public boolean hasNext() {
        return !queries.isEmpty();
    }
}
