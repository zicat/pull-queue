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

import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.name.zicat.queue.IOUtils.*;

/** SegmentBuilder. */
public class SegmentBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentBuilder.class);
    private static final String FILE_DEFAULT_PREFIX = "segment_";
    private static final String FILE_DEFAULT_SUFFIX = ".log";
    public static final int BLOCK_HEAD_SIZE = 4;
    public static final int SEGMENT_HEAD_SIZE = 8;

    private Long fileId;
    private long segmentSize = 2L * 1024L * 1024L * 1024L;
    private int blockSize = 32 * 1024;
    private File dir;
    private String filePrefix = null;
    private CompressionType compressionType = CompressionType.NONE;

    /**
     * set file id.
     *
     * @param fileId fileId
     * @return SegmentBuilder
     */
    public SegmentBuilder fileId(long fileId) {
        this.fileId = fileId;
        return this;
    }

    /**
     * set compression type.
     *
     * @param compressionType compressionType
     * @return SegmentBuilder
     */
    public SegmentBuilder compressionType(CompressionType compressionType) {
        if (compressionType != null) {
            this.compressionType = compressionType;
        }
        return this;
    }

    /**
     * set file prefix.
     *
     * @param filePrefix filePrefix
     * @return SegmentBuilder
     */
    public SegmentBuilder filePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
        return this;
    }

    /**
     * set block size.
     *
     * @param blockSize blockSize.
     * @return SegmentBuilder
     */
    public SegmentBuilder blockSize(Integer blockSize) {
        if (blockSize != null) {
            this.blockSize = blockSize;
        }
        return this;
    }

    /**
     * set segment size.
     *
     * @param segmentSize segmentSize
     * @return SegmentBuilder
     */
    public SegmentBuilder segmentSize(Long segmentSize) {
        if (segmentSize != null) {
            this.segmentSize = segmentSize;
        }
        return this;
    }

    /**
     * set dir.
     *
     * @param dir dir
     * @return SegmentBuilder
     */
    public SegmentBuilder dir(File dir) {
        this.dir = dir;
        return this;
    }

    /**
     * build segment.
     *
     * @return Segment
     */
    public Segment build(ByteBufferTuple byteBufferTuple) {
        if (fileId == null) {
            throw new NullPointerException("segment file id is null");
        }
        if (dir == null) {
            throw new NullPointerException("segment dir is null");
        }
        final File file = new File(dir, getNameById(filePrefix, fileId));
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
            long position = fileChannel.size();
            ByteBuffer byteBuffer = ByteBuffer.allocate(SEGMENT_HEAD_SIZE);
            // read block size from segment head first
            if (position == 0) {
                byteBuffer.putInt(blockSize);
                byteBuffer.put(compressionType.id);
                while (byteBuffer.hasRemaining()) {
                    byteBuffer.put((byte) 1);
                }
                byteBuffer.flip();
                position += writeFull(fileChannel, byteBuffer);
            } else {
                readFully(fileChannel, byteBuffer, 0);
                byteBuffer.flip();
                blockSize = byteBuffer.getInt();
                compressionType = CompressionType.getById(byteBuffer.get());
            }
            fileChannel.position(position);
            LOG.info(
                    "create new segment fileId:{}, segmentSize:{}, blockSize:{}",
                    file,
                    segmentSize,
                    blockSize);
            return new Segment(
                    fileId,
                    file,
                    fileChannel,
                    byteBufferTuple.reAllocate(blockSize),
                    segmentSize,
                    compressionType,
                    position);
        } catch (IOException e) {
            IOUtils.closeQuietly(fileChannel);
            IOUtils.closeQuietly(randomAccessFile);
            throw new RuntimeException("create segment error, file path " + file.getPath(), e);
        }
    }

    /** ByteBufferTuple. */
    public static class ByteBufferTuple {

        private ByteBuffer writeBlock;
        private ByteBuffer reusedBuffer;
        private int blockSize;

        public ByteBufferTuple(int blockSize) {
            this.blockSize = blockSize;
            this.writeBlock = ByteBuffer.allocateDirect(blockSize);
            this.reusedBuffer = ByteBuffer.allocateDirect(blockSize + BLOCK_HEAD_SIZE);
        }

        public ByteBuffer writeBlock() {
            return writeBlock;
        }

        public ByteBuffer reusedBuffer() {
            return reusedBuffer;
        }

        public int blockSize() {
            return blockSize;
        }

        public ByteBuffer reusedBuffer(ByteBuffer newBuffer) {
            this.reusedBuffer = newBuffer;
            return newBuffer;
        }

        public ByteBufferTuple reAllocate(int blockSize) {
            this.blockSize = blockSize;
            this.writeBlock = IOUtils.reAllocate(writeBlock, blockSize);
            this.reusedBuffer = IOUtils.reAllocate(reusedBuffer, blockSize + BLOCK_HEAD_SIZE);
            return this;
        }
    }

    /** CompressionType. */
    public enum CompressionType {
        NONE((byte) 1, "none"),
        ZSTD((byte) 2, "zstd"),
        SNAPPY((byte) 3, "snappy");

        private final byte id;
        private final String name;

        CompressionType(byte id, String name) {
            this.id = id;
            this.name = name;
        }

        public byte getId() {
            return id;
        }

        /**
         * get type by name.
         *
         * @param name name
         * @return CompressionType
         */
        public static CompressionType getByName(String name) {
            if (name == null) {
                return CompressionType.NONE;
            }
            name = name.trim().toLowerCase();
            if (NONE.name.equals(name)) {
                return NONE;
            } else if (ZSTD.name.equals(name)) {
                return ZSTD;
            } else if (SNAPPY.name.equals(name)) {
                return SNAPPY;
            } else {
                throw new IllegalStateException("compression name not found, name " + name);
            }
        }

        /**
         * get type by id.
         *
         * @param b id
         * @return CompressionType
         */
        public static CompressionType getById(byte b) {
            if (b == 1) {
                return NONE;
            } else if (b == 2) {
                return ZSTD;
            } else if (b == 3) {
                return SNAPPY;
            } else {
                throw new IllegalStateException("compression type not found, id " + b);
            }
        }

        /**
         * compression byte buffer.
         *
         * @param byteBuffer byteBuffer
         * @return byteBuffer
         */
        public ByteBuffer compression(ByteBuffer byteBuffer) throws IOException {
            if (this == ZSTD) {
                return Zstd.compress(byteBuffer, 3);
            } else if (this == NONE) {
                return byteBuffer;
            } else if (this == SNAPPY) {
                return compressionSnappy(byteBuffer);
            } else {
                throw new IllegalStateException("compression type not found, id " + getId());
            }
        }

        /**
         * decompression byte buffer.
         *
         * @param byteBuffer byteBuffer
         * @return byteBuffer
         */
        public ByteBuffer decompression(ByteBuffer byteBuffer) throws IOException {
            if (this == ZSTD) {
                int size = (int) Zstd.decompressedSize(byteBuffer.duplicate());
                return Zstd.decompress(byteBuffer, size);
            } else if (this == NONE) {
                return byteBuffer;
            } else if (this == SNAPPY) {
                return decompressionSnappy(byteBuffer);
            } else {
                throw new IllegalStateException("compression type not found, id " + getId());
            }
        }
    }

    /**
     * get fileId by fileName.
     *
     * @param fileName fileName
     * @return fileId
     */
    public static long getIdByName(String filePrefix, String fileName) {
        return Long.parseLong(
                fileName.substring(
                        realPrefix(filePrefix, FILE_DEFAULT_PREFIX).length(),
                        fileName.length() - FILE_DEFAULT_SUFFIX.length()));
    }

    /**
     * get fileName by fileId.
     *
     * @param fileId fileId
     * @return fileName
     */
    public static String getNameById(String filePrefix, long fileId) {
        return realPrefix(filePrefix, FILE_DEFAULT_PREFIX) + fileId + FILE_DEFAULT_SUFFIX;
    }

    /**
     * get real prefix.
     *
     * @param filePrefix filePrefix
     * @param defaultValue defaultValue
     * @return prefix name
     */
    public static String realPrefix(String filePrefix, String defaultValue) {
        return filePrefix == null ? defaultValue : filePrefix + "_" + defaultValue;
    }

    /**
     * check file is segment.
     *
     * @param fileName fileName
     * @return true if segment else false
     */
    public static boolean isSegment(String filePrefix, String fileName) {
        if (!fileName.startsWith(realPrefix(filePrefix, FILE_DEFAULT_PREFIX))
                || !fileName.endsWith(FILE_DEFAULT_SUFFIX)) {
            return false;
        }
        try {
            return getIdByName(filePrefix, fileName) >= 0;
        } catch (Exception e) {
            return false;
        }
    }
}
