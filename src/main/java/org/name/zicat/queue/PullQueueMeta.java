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

/** LogQueueMeta support get runtime metrics and static metrics. */
public interface PullQueueMeta {

    /**
     * get partition count.
     *
     * @return int
     */
    int partition();

    /**
     * get active segment.
     *
     * @return int
     */
    int activeSegment();

    /**
     * estimate the lag between file offset and write position in one partition.
     *
     * @param partition partition
     * @param fileOffset fileOffset
     * @return long lag
     */
    long lag(int partition, FileOffset fileOffset);

    /**
     * return all write bytes.
     *
     * @return write bytes.
     */
    long writeBytes();

    /**
     * return all read bytes.
     *
     * @return read bytes
     */
    long readBytes();

    /**
     * page cache.
     *
     * @return page cache.
     */
    long pageCache();
}
