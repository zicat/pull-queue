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

/** FileLogQueueBuilder. */
public class FilePullQueueBuilder extends PullQueueBuilder {

    private File dir;

    public FilePullQueueBuilder dir(File dir) {
        this.dir = dir;
        return this;
    }

    /**
     * build by customer groupManager.
     *
     * @param groupManager groupManager
     * @return FileLogQueue
     */
    public FilePullQueue build(GroupManager groupManager) {
        if (dir == null) {
            throw new IllegalStateException("dir is null");
        }
        if (!dir.exists() && !makeDir(dir)) {
            throw new IllegalStateException(
                    "dir not exist and try to create fail " + dir.getPath());
        }
        if (consumerGroups == null || consumerGroups.isEmpty()) {
            throw new IllegalStateException("file log queue must has at least one consumer group");
        }
        return new FilePullQueue(
                topic,
                groupManager,
                dir,
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
     * build file log queue.
     *
     * @return return
     */
    public FilePullQueue build() {
        return build(new DefaultGroupManager(dir, topic, consumerGroups));
    }

    /**
     * mkdir.
     *
     * @param dir dir
     * @return boolean make success
     */
    public static boolean makeDir(File dir) {
        if (dir.exists()) {
            return dir.isDirectory();
        }
        if (dir.getParentFile().exists()) {
            return dir.mkdir();
        } else {
            return makeDir(dir.getParentFile()) && dir.mkdir();
        }
    }

    public static FilePullQueueBuilder newBuilder() {
        return new FilePullQueueBuilder();
    }
}
