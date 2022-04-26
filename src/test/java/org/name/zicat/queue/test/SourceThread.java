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

import org.name.zicat.queue.PullQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Random;

/** ConsumerThread. */
public class SourceThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SourceThread.class);

    public static Random random = new Random(123123123L);

    final PullQueue queue;
    final int partition;
    final long dataSize;
    final byte[] data;
    final int halfMax;
    final boolean realRandom;

    public SourceThread(
            PullQueue queue,
            int partition,
            long dataSize,
            int recordMaxLength,
            boolean realRandom) {
        this.queue = queue;
        this.partition = partition;
        this.dataSize = dataSize;
        this.halfMax = recordMaxLength / 2;
        this.realRandom = realRandom;
        data = new byte[recordMaxLength];
        Random realR = getRandom();
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) realR.nextInt();
        }
    }

    public SourceThread(PullQueue queue, int partition, long dataSize, int recordMaxLength) {
        this(queue, partition, dataSize, recordMaxLength, false);
    }

    @Override
    public void run() {
        long writeLength = 0;
        long start = System.currentTimeMillis();
        DecimalFormat df = new DecimalFormat("######0.00");
        Random realR = getRandom();
        for (int id = 0; id < dataSize; id++) {
            try {
                int length = realR.nextInt(halfMax) + halfMax;
                queue.append(partition, data, 0, length);
                writeLength += length;
                long spend = System.currentTimeMillis() - start;
                if (writeLength >= 1024 * 1024 * 1024 && spend > 0) {
                    LOG.info(
                            "write spend:"
                                    + df.format(writeLength / 1024.0 / 1024.0 / (spend / 1000.0))
                                    + "(mb/s)");
                    writeLength = 0;
                    start = System.currentTimeMillis();
                }
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }
    }

    private Random getRandom() {
        return realRandom ? new Random(System.currentTimeMillis()) : random;
    }
}
