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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.name.zicat.queue.IOUtils.*;
import static org.name.zicat.queue.SegmentBuilder.BLOCK_HEAD_SIZE;
import static org.name.zicat.queue.SegmentBuilder.SEGMENT_HEAD_SIZE;
import static org.name.zicat.queue.VIntUtil.putVint;
import static org.name.zicat.queue.VIntUtil.vIntLength;

/**
 * Segment support append data to block and encode block to one file. @ThreadSafe
 *
 * <p>segment header(8 bytes): |4 bytes blockSize|1 byte CompressionType|3 bytes reserved|
 *
 * <p>segment body:|block1|block2|block3|......|
 *
 * <p>block:|4 bytes blockSize|record1|record2|......|
 *
 * <p>one record:|vint record length|record value|
 *
 * @author zicat
 */
public final class Segment implements Closeable, Comparable<Segment> {

    private static final int HALF_INTEGER_MAX = Integer.MAX_VALUE / 2;
    private final long id;
    private final File file;
    private final FileChannel fileChannel;
    private final long segmentSize;
    private final SegmentBuilder.CompressionType compressionType;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readable = lock.newCondition();
    private final AtomicLong readablePosition = new AtomicLong();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final BlockSet blockSet;
    private long pageCacheUsed = 0;

    public Segment(
            long id,
            File file,
            FileChannel fileChannel,
            BlockSet blockSet,
            long segmentSize,
            SegmentBuilder.CompressionType compressionType,
            long position) {
        if (segmentSize - SEGMENT_HEAD_SIZE < blockSet.blockSize()) {
            throw new IllegalArgumentException(
                    "segment size must over block size, segment size = "
                            + segmentSize
                            + ",block size = "
                            + blockSet.blockSize());
        }
        this.id = id;
        this.file = file;
        this.fileChannel = fileChannel;
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        this.blockSet = blockSet;
        this.readablePosition.set(position);
    }

    /**
     * append bytes to segment.
     *
     * @param data data
     * @param offset offset
     * @param length length
     * @return tuple return t1 = -1 if segment is full, return 0 if data is null or empty else
     *     return data size in channel or block
     * @throws IOException IOException
     */
    public int append(byte[] data, int offset, int length) throws IOException {

        checkOpen();
        if (length <= 0) {
            return 0;
        }
        final int dataLength = vIntLength(length);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isFinish()) {
                return -1;
            }
            if (blockSet.writeBlock().remaining() < dataLength) {
                if (blockSet.writeBlock().position() != 0) {
                    encodeBlockSetAndWriterChannel();
                }
                if (readablePosition() > segmentSize) {
                    return -1;
                }
            }
            if (blockSet.writeBlock().remaining() >= dataLength) {
                encodeData(blockSet.writeBlock(), data, offset, length);
            } else {
                final ByteBuffer bigRecordBuffer = ByteBuffer.allocateDirect(dataLength);
                encodeData(bigRecordBuffer, data, offset, length).flip();
                encodeBlockAndWriteChannel(
                        bigRecordBuffer, blockSet.compressionBlock(), blockSet.encodeBlock());
            }
        } finally {
            lock.unlock();
        }
        return dataLength;
    }

    /**
     * encode data to bytebuffer, vint + data.
     *
     * @param byteBuffer byteBuffer
     * @param data data
     * @param offset offset
     * @param length length
     */
    private ByteBuffer encodeData(ByteBuffer byteBuffer, byte[] data, int offset, int length) {
        putVint(byteBuffer, length);
        byteBuffer.put(data, offset, length);
        return byteBuffer;
    }

    /**
     * blocking read data from file channel.
     *
     * @param fileOffset offset in file
     * @param time block time
     * @param unit time unit
     * @return return null if eof or timeout, else return data
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    public DataResultSet readBlock(BlockFileOffset fileOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        checkOpen();
        if (!fileIdMatch(fileOffset)) {
            throw new IllegalStateException(
                    "segment match fail, want " + fileOffset.fileId() + " real " + fileId());
        }
        final long offset = fileOffset.offset() == 0 ? SEGMENT_HEAD_SIZE : fileOffset.offset();
        if (offset < readablePosition()) {
            return read(offset, fileOffset);
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (offset >= readablePosition() && !finished.get()) {
                if (time == 0) {
                    readable.await();
                } else if (!readable.await(time, unit)) {
                    return new DataResultSet(fileOffset, 0);
                }
            } else if (offset >= readablePosition()) {
                return new DataResultSet(fileOffset, 0);
            }
        } finally {
            lock.unlock();
        }
        return read(offset, fileOffset);
    }

    /**
     * read data from file channel.
     *
     * @param offset offset
     * @return return current fileOffset if reach read position , else return data
     * @throws IOException IOException
     */
    private DataResultSet read(long offset, BlockFileOffset fileOffset) throws IOException {

        final ByteBuffer reusedDataLengthBuffer =
                reAllocate(fileOffset.getReusedDataLengthBuffer(), BLOCK_HEAD_SIZE);
        readFully(fileChannel, reusedDataLengthBuffer, offset);
        reusedDataLengthBuffer.flip();

        offset += BLOCK_HEAD_SIZE;
        final int limit = reusedDataLengthBuffer.getInt();
        final int capacity = limit < HALF_INTEGER_MAX ? limit << 1 : Integer.MAX_VALUE;
        final ByteBuffer reusedDataBodyBuffer =
                reAllocate(fileOffset.getReusedDataBodyBuffer(), capacity, limit);
        readFully(fileChannel, reusedDataBodyBuffer, offset);
        reusedDataBodyBuffer.flip();

        final BlockFileOffset blockFileOffset =
                new BlockFileOffset(
                        fileId(),
                        offset + reusedDataBodyBuffer.limit(),
                        compressionType.decompression(
                                reusedDataBodyBuffer, fileOffset.getDataBuffer()),
                        reusedDataBodyBuffer,
                        reusedDataLengthBuffer);
        return new DataResultSet(blockFileOffset, limit + BLOCK_HEAD_SIZE);
    }

    /**
     * check file channel whether open.
     *
     * @throws IOException IOException
     */
    private void checkOpen() throws IOException {
        if (!fileChannel.isOpen()) {
            throw new IOException("file is close, file path = " + filePath());
        }
    }

    /**
     * encode block.
     *
     * @param byteBuffer byteBuffer
     * @param compressionBlock compressionBlock
     * @param encodeBlock encodeBlock
     */
    private void encodeBlockAndWriteChannel(
            ByteBuffer byteBuffer, ByteBuffer compressionBlock, ByteBuffer encodeBlock)
            throws IOException {
        final ByteBuffer compressionBuffer =
                compressionType.compression(byteBuffer, compressionBlock);
        blockSet.compressionBlock(compressionBuffer);
        final int size = compressionBuffer.limit() + BLOCK_HEAD_SIZE;
        encodeBlock = reAllocate(encodeBlock, size);
        encodeBlock.putInt(compressionBuffer.limit());
        encodeBlock.put(compressionBuffer).flip();
        blockSet.encodeBlock(encodeBlock);
        final int readableCount = writeFull(fileChannel, encodeBlock);
        readablePosition.addAndGet(readableCount);
        pageCacheUsed += readableCount;
        readable.signalAll();
    }

    /**
     * encode block set and writer channel.
     *
     * @throws IOException IOException
     */
    private void encodeBlockSetAndWriterChannel() throws IOException {
        blockSet.writeBlock().flip();
        encodeBlockAndWriteChannel(
                blockSet.writeBlock(), blockSet.compressionBlock(), blockSet.encodeBlock());
        blockSet.writeBlock().clear();
    }

    /**
     * check fileOffset whether in this segment.
     *
     * @param fileOffset fileOffset
     * @return boolean match
     */
    public final boolean fileIdMatch(FileOffset fileOffset) {
        return fileOffset != null && this.fileId() == fileOffset.fileId();
    }

    /**
     * return page cache used count.
     *
     * @return long
     */
    public final long pageCacheUsed() {
        return pageCacheUsed;
    }

    /**
     * file id.
     *
     * @return fileId
     */
    public final long fileId() {
        return id;
    }

    /**
     * flush data.
     *
     * @throws IOException IOException
     */
    public void flush() throws IOException {
        if (isFinish()) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isFinish()) {
                return;
            }
            if (blockSet.writeBlock().position() != 0) {
                encodeBlockSetAndWriterChannel();
                fileChannel.force(true);
            }
            pageCacheUsed = 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * set segment as not writeable, but read is allow.
     *
     * @throws IOException IOException
     */
    public synchronized void finish() throws IOException {
        if (isFinish()) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isFinish()) {
                return;
            }
            flush();
            finished.set(true);
            readable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * whether finish.
     *
     * @return true if segment is not allow to write
     */
    public boolean isFinish() {
        return finished.get();
    }

    /** delete segment. */
    public synchronized boolean delete() {
        IOUtils.closeQuietly(this);
        return file.delete();
    }

    @Override
    public synchronized void close() throws IOException {
        if (fileChannel.isOpen()) {
            try {
                finish();
            } finally {
                IOUtils.closeQuietly(fileChannel);
            }
        }
    }

    @Override
    public int compareTo(Segment o) {
        return Long.compare(this.fileId(), o.fileId());
    }

    /**
     * estimate lag, the lag not contains data in block.
     *
     * @param fileOffset fileOffset
     * @return long lag
     */
    public final long estimateLag(FileOffset fileOffset) {
        if (fileIdMatch(fileOffset)) {
            final long lag = readablePosition() - fileOffset.offset();
            return lag < 0 ? 0 : lag;
        } else {
            final long fileIdDelta = this.fileId() - fileOffset.fileId();
            if (fileIdDelta < 0) {
                return 0;
            }
            final long segmentLag = (fileIdDelta - 1) * segmentSize;
            // may segment size adjust, this lag is only an estimation
            final long lagInSegment = readablePosition() + (segmentSize - fileOffset.offset());
            return segmentLag + lagInSegment;
        }
    }

    /**
     * get readable position.
     *
     * @return long
     */
    public final long readablePosition() {
        return readablePosition.get();
    }

    /**
     * file path.
     *
     * @return string
     */
    public String filePath() {
        return file.getPath();
    }
}
