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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.name.zicat.queue.SegmentBuilder.getIdByName;
import static org.name.zicat.queue.SegmentBuilder.isSegment;

/**
 * FilePullQueue to Storage data in file.
 *
 * <p>@ThreadSafe
 */
public class FilePullQueue implements PullQueue, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FilePullQueue.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newSegmentCondition = lock.newCondition();
    protected final Map<Long, Segment> segmentCache = new ConcurrentHashMap<>();
    protected final File dir;
    protected final Long segmentSize;
    protected final SegmentBuilder.CompressionType compressionType;
    protected final Integer blockSize;
    protected final GroupManager groupManager;
    private volatile Segment lastSegment;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long flushPeriodMill;
    private long cleanupMill;
    private Thread cleanUpThread;
    private Thread flushSegmentThread;
    private final String topic;
    private final long flushPageCacheSize;
    private final SegmentBuilder.ByteBufferTuple byteBufferTuple;
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();

    protected FilePullQueue(
            String topic,
            GroupManager groupManager,
            File dir,
            Integer blockSize,
            Long segmentSize,
            SegmentBuilder.CompressionType compressionType,
            long cleanUpPeriod,
            TimeUnit cleanUpUnit,
            long flushPeriod,
            TimeUnit flushUnit,
            long flushPageCacheSize) {
        this.topic = topic;
        this.dir = dir;
        this.blockSize = blockSize;
        this.byteBufferTuple = new SegmentBuilder.ByteBufferTuple(blockSize);
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        this.groupManager = groupManager;
        this.flushPageCacheSize = flushPageCacheSize;
        loadSegments();
        if (cleanUpPeriod > 0) {
            cleanupMill = cleanUpUnit.toMillis(cleanUpPeriod);
            cleanUpThread = new Thread(this::periodCleanUp, "cleanup_segment_thread");
            cleanUpThread.start();
        }
        if (flushPeriod > 0) {
            flushPeriodMill = flushUnit.toMillis(flushPeriod);
            flushSegmentThread = new Thread(this::periodForceSegment, "segment_flush_thread");
            flushSegmentThread.start();
        }
    }

    /** load segments. */
    private void loadSegments() {
        final File[] files = dir.listFiles(file -> isSegment(topic, file.getName()));
        if (files == null || files.length == 0) {
            // create new log segment if dir is empty, start id = 0
            this.lastSegment = createSegment(0);
            return;
        }

        // find max id as segment id
        final Segment maxSegment =
                Arrays.stream(files)
                        .map(
                                file -> {
                                    final String fileName = file.getName();
                                    final long id = getIdByName(topic, fileName);
                                    return createSegment(id);
                                })
                        .max(Segment::compareTo)
                        .get();
        for (Map.Entry<Long, Segment> entry : segmentCache.entrySet()) {
            final Segment segment = entry.getValue();
            // set history segment as finish, maybe read is delay and blocking
            if (segment != maxSegment) {
                try {
                    segment.finish();
                } catch (IOException e) {
                    LOG.warn("finish segment error", e);
                }
            }
        }
        this.lastSegment = maxSegment;
        cleanUp();
    }

    /**
     * create segment by id.
     *
     * @param id id
     * @return Segment
     */
    private Segment createSegment(long id) {
        final Segment segment =
                new SegmentBuilder()
                        .fileId(id)
                        .dir(dir)
                        .blockSize(blockSize)
                        .segmentSize(segmentSize)
                        .filePrefix(topic)
                        .compressionType(compressionType)
                        .build(byteBufferTuple);
        segmentCache.put(id, segment);
        return segment;
    }

    @Override
    public void append(int partition, byte[] data, int offset, int length) throws IOException {
        appendIgnorePartition(data, offset, length);
    }

    /**
     * append data without partition.
     *
     * @param data data
     * @throws IOException IOException
     */
    private void appendIgnorePartition(byte[] data, int offset, int length) throws IOException {

        final Segment segment = this.lastSegment;
        if (segment.append(data, offset, length) != -1) {
            checkSyncForce(segment);
        } else {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                segment.finish();
                final Segment retrySegment = this.lastSegment;
                if (retrySegment.append(data, offset, length) == -1) {
                    retrySegment.finish();
                    final Segment newSegment = createSegment(retrySegment.fileId() + 1L);
                    newSegment.append(data, offset, length);
                    writeBytes.addAndGet(lastSegment.readablePosition());
                    this.lastSegment = newSegment;
                    newSegmentCondition.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * check whether sync flush.
     *
     * @param segment segment
     */
    private void checkSyncForce(Segment segment) throws IOException {
        if (segment.pageCacheUsed() >= flushPageCacheSize) {
            segment.flush();
        }
    }

    @Override
    public long lag(int partition, FileOffset fileOffset) {
        return lastSegment.estimateLag(fileOffset);
    }

    @Override
    public long writeBytes() {
        return writeBytes.get() + lastSegment.readablePosition();
    }

    @Override
    public long readBytes() {
        return readBytes.get();
    }

    @Override
    public long pageCache() {
        return lastSegment.pageCacheUsed();
    }

    @Override
    public DataResultSet poll(int partition, BlockFileOffset offset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        final DataResultSet resultSet = read(offset, time, unit);
        readBytes.addAndGet(resultSet.readBytes());
        return resultSet;
    }

    @Override
    public BlockFileOffset getFileOffset(String groupId, int partition) {
        return new BlockFileOffset(groupManager.getFileOffset(groupId));
    }

    @Override
    public void commitFileOffset(String groupId, int partition, FileOffset fileOffset)
            throws IOException {
        groupManager.commitFileOffset(groupId, fileOffset);
    }

    /**
     * read file log.
     *
     * @param fileOffset fileOffset
     * @param time time
     * @param unit unit
     * @return LogResult
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    private DataResultSet read(BlockFileOffset fileOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        final Segment currentSegment = this.lastSegment;
        final BlockFileOffset realFileOffset = createDefaultFileOffset(fileOffset);
        final Segment searchSegment =
                currentSegment.fileIdMatch(realFileOffset)
                        ? currentSegment
                        : segmentCache.get(realFileOffset.fileId());
        if (searchSegment == null) {
            throw new IOException("segment not found segment id = " + realFileOffset.fileId());
        }
        // try read it first
        final DataResultSet resultBuffer = searchSegment.readBlock(realFileOffset, time, unit);
        if (!searchSegment.isFinish() || resultBuffer.hasNext()) {
            return resultBuffer;
        }

        // searchSegment is full
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // searchSegment point to currentSegment, await new segment
            if (searchSegment == this.lastSegment) {
                if (time == 0) {
                    newSegmentCondition.await();
                } else if (!newSegmentCondition.await(time, unit)) {
                    return new DataResultSet(realFileOffset, 0);
                }
            }
        } finally {
            lock.unlock();
        }

        final BlockFileOffset nextFileOffset =
                new BlockFileOffset(
                        realFileOffset.fileId() + 1,
                        0,
                        realFileOffset.getDataBuffer(),
                        realFileOffset.getReusedDataBodyBuffer(),
                        realFileOffset.getReusedDataLengthBuffer());
        final Segment nextSegment = segmentCache.get(nextFileOffset.fileId());
        return nextSegment.readBlock(nextFileOffset, time, unit);
    }

    /**
     * read from beginning if null.
     *
     * @param fileOffset fileOffset
     * @return FileOffset
     */
    protected BlockFileOffset createDefaultFileOffset(BlockFileOffset fileOffset) {
        if (fileOffset == null || fileOffset.fileId() < 0) {
            Segment minSegment = lastSegment;
            for (Map.Entry<Long, Segment> entry : segmentCache.entrySet()) {
                if (minSegment.fileId() > entry.getValue().fileId()) {
                    minSegment = entry.getValue();
                }
            }
            fileOffset = new BlockFileOffset(minSegment.fileId(), 0);
        }
        return fileOffset;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            IOUtils.closeQuietly(groupManager);
            segmentCache.forEach((k, v) -> IOUtils.closeQuietly(v));
            segmentCache.clear();
            if (cleanUpThread != null) {
                cleanUpThread.interrupt();
            }
            if (flushSegmentThread != null) {
                flushSegmentThread.interrupt();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        lastSegment.flush();
    }

    /**
     * get topic.
     *
     * @return topic
     */
    public final String getTopic() {
        return topic;
    }

    /**
     * loopCloseableFunction.
     *
     * @param function function
     * @param period period
     */
    public static void loopCloseableFunction(
            Function<Object, Object> function, long period, AtomicBoolean closed) {

        sleepQuietly(period);
        Object result = null;
        while (!closed.get()) {
            final long start = System.currentTimeMillis();
            try {
                result = function.apply(result);
            } catch (Throwable e) {
                LOG.warn("call back error", e);
            }
            final long waitTime = period - (System.currentTimeMillis() - start);
            final long leastWait = waitTime <= 0 ? 10 : waitTime;
            sleepQuietly(leastWait);
        }
    }

    /** force segment to dish. */
    protected void periodForceSegment() {
        loopCloseableFunction(
                t -> {
                    try {
                        flush();
                        return true;
                    } catch (Exception e) {
                        LOG.warn("period flush error", e);
                        return false;
                    }
                },
                flushPeriodMill,
                closed);
    }

    /**
     * clean up old segment.
     *
     * @return boolean clean up
     */
    protected boolean cleanUp() {
        final FileOffset min = groupManager.getMinFileOffset();
        if (min == null || min.fileId() < 0) {
            return false;
        }
        final List<Segment> expiredSegments = new ArrayList<>();
        for (Map.Entry<Long, Segment> entry : segmentCache.entrySet()) {
            final Segment segment = entry.getValue();
            if (segment.fileId() < min.fileId()) {
                expiredSegments.add(segment);
            }
        }
        for (Segment segment : expiredSegments) {
            segmentCache.remove(segment.fileId());
            if (segment.delete()) {
                LOG.info("expired file " + segment.filePath() + " deleted success");
            } else {
                LOG.info("expired file " + segment.filePath() + " deleted fail");
            }
        }
        return true;
    }

    /** clean up expired log. */
    protected void periodCleanUp() {
        loopCloseableFunction(t -> cleanUp(), cleanupMill, closed);
    }

    /**
     * sleep ignore interrupted.
     *
     * @param period period
     */
    private static void sleepQuietly(long period) {
        try {
            Thread.sleep(period);
        } catch (InterruptedException ignore) {
        }
    }

    @Override
    public int partition() {
        return 1;
    }

    @Override
    public int activeSegment() {
        return segmentCache.size();
    }
}
