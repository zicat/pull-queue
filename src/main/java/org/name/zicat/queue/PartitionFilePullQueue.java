/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.name.zicat.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.name.zicat.queue.FilePullQueue.loopCloseableFunction;

/**
 * PartitionFilePullQueue maintain FilePull Queue list and support partition related
 * operation. @ThreadSafe
 */
public class PartitionFilePullQueue implements PullQueue, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionFilePullQueue.class);
    private final FilePullQueue[] pullQueues;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread cleanUpThread;
    private final Thread flushSegmentThread;
    private final long cleanupMill;
    private final long flushPeriodMill;

    protected PartitionFilePullQueue(
            String topic,
            List<String> consumerGroups,
            List<File> dirs,
            Integer blockSize,
            Long segmentSize,
            SegmentBuilder.CompressionType compressionType,
            long cleanUpPeriod,
            TimeUnit cleanUpUnit,
            long flushPeriod,
            TimeUnit flushUnit,
            long flushPageSize) {

        pullQueues = new FilePullQueue[dirs.size()];
        FilePullQueueBuilder builder = FilePullQueueBuilder.newBuilder();
        builder.segmentSize(segmentSize)
                .blockSize(blockSize)
                .compressionType(compressionType)
                .flushPeriod(0, flushUnit)
                .flushPageCacheSize(flushPageSize)
                .topic(topic)
                .consumerGroups(consumerGroups)
                .cleanUpPeriod(0, cleanUpUnit);
        for (int i = 0; i < dirs.size(); i++) {
            FilePullQueue pullQueue = builder.dir(dirs.get(i)).build();
            pullQueues[i] = pullQueue;
        }
        cleanupMill = cleanUpUnit.toMillis(cleanUpPeriod);
        cleanUpThread = new Thread(this::cleanUp, "cleanup_segment_thread");
        cleanUpThread.start();

        flushPeriodMill = flushUnit.toMillis(flushPeriod);
        flushSegmentThread = new Thread(this::periodForceSegment, "segment_flush_thread");
        flushSegmentThread.start();
    }

    /** force segment to dish. */
    protected void periodForceSegment() {
        loopCloseableFunction(
                t -> {
                    try {
                        flush();
                    } catch (Exception e) {
                        LOG.warn("period flush log queue error", e);
                    }
                    return null;
                },
                flushPeriodMill,
                closed);
    }

    /** clean up task. */
    protected void cleanUp() {
        loopCloseableFunction(
                t -> {
                    for (FilePullQueue logQueue : pullQueues) {
                        logQueue.cleanUp();
                    }
                    return null;
                },
                cleanupMill,
                closed);
    }

    @Override
    public void append(int partition, byte[] data, int offset, int length) throws IOException {
        final FilePullQueue queue = getPartitionQueue(partition);
        queue.append(partition, data, offset, length);
    }

    @Override
    public LogResultSet poll(int partition, BlockFileOffset logOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        final FilePullQueue queue = getPartitionQueue(partition);
        return queue.poll(partition, logOffset, time, unit);
    }

    @Override
    public BlockFileOffset getFileOffset(String groupId, int partition) {
        final FilePullQueue queue = getPartitionQueue(partition);
        return queue.getFileOffset(groupId, partition);
    }

    @Override
    public void commitFileOffset(String groupId, int partition, FileOffset fileOffset)
            throws IOException {
        final FilePullQueue queue = getPartitionQueue(partition);
        queue.commitFileOffset(groupId, partition, fileOffset);
    }

    @Override
    public long lag(int partition, FileOffset fileOffset) {
        final FilePullQueue queue = getPartitionQueue(partition);
        return queue.lag(partition, fileOffset);
    }

    /**
     * check partition valid.
     *
     * @param partition partition
     */
    private FilePullQueue getPartitionQueue(int partition) {
        if (partition >= pullQueues.length) {
            throw new IllegalArgumentException(
                    "partition out of range, max partition "
                            + (pullQueues.length - 1)
                            + " input partition "
                            + partition);
        }
        return pullQueues[partition];
    }

    @Override
    public void flush() throws IOException {
        for (FilePullQueue queue : pullQueues) {
            queue.flush();
        }
    }

    /**
     * flush one partition.
     *
     * @param partition partition
     * @throws IOException IOException
     */
    public void flush(int partition) throws IOException {
        final FilePullQueue queue = getPartitionQueue(partition);
        queue.flush();
    }

    @Override
    public int partition() {
        return pullQueues.length;
    }

    @Override
    public int activeSegment() {
        return Arrays.stream(pullQueues).mapToInt(PullQueue::activeSegment).sum();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (PullQueue pullQueue : pullQueues) {
                IOUtils.closeQuietly(pullQueue);
            }
            cleanUpThread.interrupt();
            flushSegmentThread.interrupt();
        }
    }

    @Override
    public long writeBytes() {
        return Arrays.stream(pullQueues).mapToLong(PullQueue::writeBytes).sum();
    }

    @Override
    public long readBytes() {
        return Arrays.stream(pullQueues).mapToLong(PullQueue::readBytes).sum();
    }

    @Override
    public long pageCache() {
        return Arrays.stream(pullQueues).mapToLong(PullQueue::pageCache).sum();
    }
}
