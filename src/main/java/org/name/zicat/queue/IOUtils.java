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
import org.xerial.snappy.Snappy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

/** IOUtils. */
public class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

    /**
     * compression snappy.
     *
     * @param byteBuffer byteBuffer
     * @return byteBuffer
     * @throws IOException IOException
     */
    public static ByteBuffer compressionSnappy(ByteBuffer byteBuffer) throws IOException {
        final int size = byteBuffer.remaining();
        final byte[] buf = new byte[Snappy.maxCompressedLength(size)];
        if (byteBuffer.hasArray()) {
            final int compressedByteSize =
                    Snappy.rawCompress(byteBuffer.array(), byteBuffer.position(), size, buf, 0);
            return ByteBuffer.wrap(buf, 0, compressedByteSize);
        } else {
            final byte[] originBytes = new byte[size];
            byteBuffer.get(originBytes);
            final int compressedByteSize =
                    Snappy.rawCompress(originBytes, 0, originBytes.length, buf, 0);
            return ByteBuffer.wrap(buf, 0, compressedByteSize);
        }
    }

    /**
     * decompression snappy.
     *
     * @param byteBuffer byteBuffer
     * @return byteBuffer
     * @throws IOException IOException
     */
    public static ByteBuffer decompressionSnappy(ByteBuffer byteBuffer) throws IOException {
        final int dataSize = byteBuffer.remaining();
        byte[] result;
        if (byteBuffer.hasArray()) {
            int size =
                    Snappy.uncompressedLength(byteBuffer.array(), byteBuffer.position(), dataSize);
            result = new byte[size];
            Snappy.uncompress(byteBuffer.array(), 0, dataSize, result, 0);
        } else {
            final byte[] originBytes = new byte[dataSize];
            byteBuffer.get(originBytes);
            int size = Snappy.uncompressedLength(originBytes);
            result = new byte[size];
            Snappy.uncompress(originBytes, 0, originBytes.length, result, 0);
        }
        return ByteBuffer.wrap(result);
    }

    /**
     * reAllocate buffer.
     *
     * @param oldBuffer old buffer
     * @param capacity capacity
     * @param limit limit
     * @return new buffer
     */
    public static ByteBuffer reAllocate(ByteBuffer oldBuffer, int capacity, int limit) {

        if (oldBuffer == null || oldBuffer.capacity() < capacity) {
            oldBuffer = ByteBuffer.allocateDirect(capacity);
        } else {
            oldBuffer.clear();
        }
        oldBuffer.limit(limit);
        return oldBuffer;
    }

    /**
     * reAllocate buffer.
     *
     * @param oldBuffer old buffer
     * @param capacity capacity
     * @return new buffer
     */
    public static ByteBuffer reAllocate(ByteBuffer oldBuffer, int capacity) {
        return reAllocate(oldBuffer, capacity, capacity);
    }

    /**
     * close quietly.
     *
     * @param closeable closeable
     */
    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            LOG.warn("close error", e);
        }
    }

    /**
     * read channel to byte buffer.
     *
     * @param channel file channel
     * @param byteBuffer buffer
     * @param position file position
     * @throws IOException IOException
     */
    public static void readFully(FileChannel channel, ByteBuffer byteBuffer, long position)
            throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException(
                    "The file channel position cannot be negative, but it is " + position);
        }
        long offset = position;
        int readCount;
        do {
            readCount = channel.read(byteBuffer, offset);
            offset += readCount;
        } while (readCount != -1 && byteBuffer.hasRemaining());
    }

    /**
     * write bytebuffer to channel.
     *
     * @param channel channel
     * @param byteBuffer ByteBuffer
     * @return write count
     * @throws IOException IOException
     */
    public static int writeFull(GatheringByteChannel channel, ByteBuffer byteBuffer)
            throws IOException {
        int count = 0;
        while (count < byteBuffer.limit()) {
            count += channel.write(byteBuffer);
        }
        return count;
    }
}
