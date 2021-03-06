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

package org.name.zicat.queue.test;

import org.name.zicat.queue.BlockFileOffset;
import org.name.zicat.queue.DataResultSet;
import org.name.zicat.queue.PullQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SinkThread.
 *
 * @author zicat
 */
public class SinkThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SinkThread.class);

    private final PullQueue queue;
    private final int partitionId;
    private final String groupName;
    private final AtomicLong readSize;
    private final long totalSize;

    public SinkThread(
            PullQueue queue,
            int partitionId,
            String groupName,
            AtomicLong readSize,
            long totalSize) {
        this.queue = queue;
        this.partitionId = partitionId;
        this.groupName = groupName;
        this.readSize = readSize;
        this.totalSize = totalSize;
    }

    @Override
    public void run() {

        try {
            BlockFileOffset fileOffset = queue.getFileOffset(groupName, partitionId);
            DataResultSet result = null;
            long readLength = 0;
            long start = System.currentTimeMillis();
            DecimalFormat df = new DecimalFormat("######0.00");
            Long preFileId = null;
            while (readSize.get() < totalSize || (result != null && result.hasNext())) {
                result = queue.poll(partitionId, fileOffset, 10, TimeUnit.MILLISECONDS);
                while (result.hasNext()) {
                    readLength += result.next().length;
                    readSize.incrementAndGet();
                    if (preFileId == null) {
                        preFileId = result.nexBlockOffset().fileId();
                    } else if (preFileId != result.nexBlockOffset().fileId()) {
                        queue.commitFileOffset(groupName, partitionId, fileOffset);
                        preFileId = result.nexBlockOffset().fileId();
                    }
                }
                long spend = System.currentTimeMillis() - start;
                if (readLength >= 1024 * 1024 * 1024 && spend > 0) {
                    LOG.info(
                            "read spend:"
                                    + df.format(readLength / 1024.0 / 1024.0 / (spend / 1000.0))
                                    + "(mb/s), file id:"
                                    + result.nexBlockOffset().fileId()
                                    + ", lag:"
                                    + queue.lag(partitionId, result.nexBlockOffset()));
                    readLength = 0;
                    start = System.currentTimeMillis();
                }
                fileOffset = result.nexBlockOffset();
            }
            queue.commitFileOffset(groupName, partitionId, fileOffset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
