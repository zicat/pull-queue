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

import java.util.List;
import java.util.concurrent.TimeUnit;

/** FileLogQueueBuilder. */
public class PullQueueBuilder {

    protected String topic;
    protected Long segmentSize;
    protected Integer blockSize;
    protected SegmentBuilder.CompressionType compressionType;
    protected long cleanUpPeriod = 5;
    protected TimeUnit cleanUpUnit = TimeUnit.MINUTES;

    protected long flushPeriod = 1;
    protected TimeUnit flushTimeUnit = TimeUnit.SECONDS;
    protected long flushPageCacheSize = 1024L * 1024L * 32L;

    protected List<String> consumerGroups;

    public PullQueueBuilder cleanUpPeriod(long cleanUpPeriod, TimeUnit cleanUpUnit) {
        this.cleanUpPeriod = cleanUpPeriod;
        this.cleanUpUnit = cleanUpUnit;
        return this;
    }

    /**
     * set compression type.
     *
     * @param compressionType compressionType
     * @return LogSegmentBuilder
     */
    public PullQueueBuilder compressionType(SegmentBuilder.CompressionType compressionType) {
        if (compressionType != null) {
            this.compressionType = compressionType;
        }
        return this;
    }

    public PullQueueBuilder flushPageCacheSize(long flushPageCacheSize) {
        this.flushPageCacheSize = flushPageCacheSize;
        return this;
    }

    public PullQueueBuilder flushPeriod(long flushPeriod, TimeUnit flushTimeUnit) {
        this.flushPeriod = flushPeriod;
        this.flushTimeUnit = flushTimeUnit;
        return this;
    }

    public PullQueueBuilder segmentSize(Long segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    public PullQueueBuilder blockSize(Integer blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public PullQueueBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public PullQueueBuilder consumerGroups(List<String> consumerGroups) {
        this.consumerGroups = consumerGroups;
        return this;
    }
}
