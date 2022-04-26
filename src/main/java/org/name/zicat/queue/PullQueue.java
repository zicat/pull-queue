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
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A PullQueue support append data to queue with partition, flush block and page cache data to disk,
 * pull data from queue by BlockFileOffset. @ThreadSafe
 *
 * @author zicat
 */
public interface PullQueue extends Closeable, PullQueueMeta {

    /**
     * append data to queue.
     *
     * <p>append operator only make sure put data to memory block or page cache.
     *
     * <p>invoke {@link PullQueue#flush()} will flush data from memory block and page cache to disk.
     *
     * @param partition partition
     * @param data data
     * @param offset offset the data offset
     * @param length length the data length to append
     * @throws IOException IOException
     */
    void append(int partition, byte[] data, int offset, int length) throws IOException;

    default void append(int partition, byte[] data) throws IOException {
        if (data != null) {
            append(partition, data, 0, data.length);
        }
    }

    /**
     * poll data.
     *
     * @param partition partition
     * @param offset offset
     * @param time time
     * @param unit unit
     * @return DataResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    DataResultSet poll(int partition, BlockFileOffset offset, long time, TimeUnit unit)
            throws IOException, InterruptedException;

    /**
     * read data. waiting if necessary * until an element becomes available.
     *
     * @param partition the partition to read
     * @param offset offset
     * @return DataResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    default DataResultSet take(int partition, BlockFileOffset offset)
            throws IOException, InterruptedException {
        return poll(partition, offset, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * get current file offset by group id & partition.
     *
     * @param groupId groupId
     * @param partition partition
     * @return BlockFileOffset
     */
    BlockFileOffset getFileOffset(String groupId, int partition);

    /**
     * commit file offset.
     *
     * @param groupId groupId
     * @param partition partition
     * @param fileOffset fileOffset
     * @throws IOException IOException
     */
    void commitFileOffset(String groupId, int partition, FileOffset fileOffset) throws IOException;

    /** flush block data and page cache data to disk. */
    void flush() throws IOException;
}
