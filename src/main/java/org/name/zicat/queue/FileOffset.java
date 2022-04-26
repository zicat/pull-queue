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

import java.nio.ByteBuffer;

/** The File Offset maintain the position of one record(or block) in File Queue. */
public class FileOffset implements Comparable<FileOffset> {

    private final long fileId;
    private final long offset;

    public FileOffset(long fileId, long offset) {
        this.fileId = fileId;
        this.offset = offset;
    }

    /**
     * get file id.
     *
     * @return file path
     */
    public long fileId() {
        return fileId;
    }

    /**
     * get the offset of the one record(or block) in fileId file.
     *
     * @return file position
     */
    public long offset() {
        return offset;
    }

    /**
     * encode file offset in byte buffer.
     *
     * @see FileOffset#parserByteBuffer(ByteBuffer) how to decode file offset from byte buffer
     * @param byteBuffer byteBuffer
     */
    public void fillBuffer(ByteBuffer byteBuffer) {
        byteBuffer.putLong(fileId);
        byteBuffer.putLong(offset);
        byteBuffer.flip();
    }

    /**
     * parse file offset from byte buffer.
     *
     * @see FileOffset#fillBuffer(ByteBuffer) how to encode file offset to byteBuffer.
     * @param byteBuffer byte buffer
     * @return FileOffset
     */
    public static FileOffset parserByteBuffer(ByteBuffer byteBuffer) {
        return new FileOffset(byteBuffer.getLong(), byteBuffer.getLong());
    }

    @Override
    public int compareTo(FileOffset o) {
        if (this.fileId == o.fileId) {
            return Long.compare(this.offset, o.offset);
        } else {
            return Long.compare(this.fileId, o.fileId);
        }
    }

    @Override
    public String toString() {
        return "FileOffset{" + "fileId=" + fileId + ", offset=" + offset + '}';
    }
}
