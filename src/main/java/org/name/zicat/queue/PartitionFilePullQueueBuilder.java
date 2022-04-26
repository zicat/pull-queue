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

import java.io.File;
import java.util.List;

/** PartitionFileLogQueueBuilder. */
public class PartitionFilePullQueueBuilder extends PullQueueBuilder {

    private List<File> dirs;

    /**
     * set dir list.
     *
     * @param dirs dir list
     * @return this
     */
    public PartitionFilePullQueueBuilder dirs(List<File> dirs) {
        this.dirs = dirs;
        return this;
    }

    /**
     * build partition file pull queue.
     *
     * @return PartitionFilePullQueue
     */
    public PartitionFilePullQueue build() {
        if (dirs == null || dirs.isEmpty()) {
            throw new IllegalStateException("dir list is null or empty");
        }
        return new PartitionFilePullQueue(
                topic,
                consumerGroups,
                dirs,
                blockSize,
                segmentSize,
                compressionType,
                cleanUpPeriod,
                cleanUpUnit,
                flushPeriod,
                flushTimeUnit,
                flushPageCacheSize);
    }

    /**
     * new build.
     *
     * @return PartitionFilePullQueueBuilder
     */
    public static PartitionFilePullQueueBuilder newBuilder() {
        return new PartitionFilePullQueueBuilder();
    }
}
