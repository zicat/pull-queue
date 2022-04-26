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

import org.junit.Assert;
import org.junit.Test;
import org.name.zicat.queue.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Test.
 *
 * @author zicat
 */
public class FilePullQueueTest {

    private static final Logger LOG = LoggerFactory.getLogger(FilePullQueueTest.class);

    @Test
    public void testDataCorrection() throws IOException, InterruptedException {
        final String dir = "/tmp/test/test_log_21/partition-";
        final int blockSize = 28;
        final long segmentSize = 1024L * 512L;
        final String groupId = "group_1";
        final String topic = "topic_21";
        final PullQueue pullQueue =
                createQueue(
                        topic, Collections.singletonList(groupId), 1, dir, segmentSize, blockSize);
        final String value = "test_data";
        pullQueue.append(0, value.getBytes(StandardCharsets.UTF_8));
        pullQueue.flush();
        BlockFileOffset offset = pullQueue.getFileOffset(groupId, 0);
        DataResultSet resultSet = pullQueue.take(0, offset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value, new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());
        pullQueue.commitFileOffset(groupId, 0, resultSet.nexBlockOffset());

        String value2 = "test_data2";
        pullQueue.append(0, value2.getBytes(StandardCharsets.UTF_8));
        pullQueue.flush();
        BlockFileOffset nextOffset = resultSet.nexBlockOffset();
        resultSet = pullQueue.take(0, nextOffset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value2, new String(resultSet.next(), StandardCharsets.UTF_8));

        resultSet = pullQueue.take(0, nextOffset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value2, new String(resultSet.next(), StandardCharsets.UTF_8));
        pullQueue.commitFileOffset(groupId, 0, resultSet.nexBlockOffset());
        IOUtils.closeQuietly(pullQueue);
    }

    @Test
    public void testSmallSegmentSize() throws IOException {
        final String dir = "/tmp/test/test_log_5/partition-";
        final int partitionCount = 1;
        final long dataSize = 500001;
        final int sinkGroups = 2;
        final int blockSize = 28;
        final long segmentSize = 1024L * 512L;
        final int maxRecordLength = 32;
        for (int i = 0; i < 5; i++) {
            test(
                    dir,
                    partitionCount,
                    dataSize,
                    sinkGroups,
                    blockSize,
                    segmentSize,
                    maxRecordLength,
                    true);
        }
    }

    @Test
    public void testPartitionAndSinkGroups() throws IOException {
        final String dir = "/tmp/test/test_log_3/partition-";
        final int partitionCount = 3;
        final long dataSize = 500000;
        final int sinkGroups = 4;
        final int blockSize = 32 * 1024;
        final long segmentSize = 1024L * 1024L * 512;
        final int maxRecordLength = 1024;
        for (int i = 0; i < 5; i++) {
            test(
                    dir,
                    partitionCount,
                    dataSize,
                    sinkGroups,
                    blockSize,
                    segmentSize,
                    maxRecordLength,
                    false);
        }
    }

    @Test
    public void testTake() throws IOException {
        final String dir = "/tmp/test/test_log_4/partition-";
        final int partitionCount = 1;
        final long dataSize = 1000000;
        final int blockSize = 32 * 1024;
        final long segmentSize = 1024L * 1024L * 125;
        final int consumerGroupCount = 5;
        final List<String> consumerGroups = new ArrayList<>();
        for (int i = 0; i < consumerGroupCount; i++) {
            consumerGroups.add("group_" + i);
        }
        final PullQueue pullQueue =
                createQueue("counter", consumerGroups, partitionCount, dir, segmentSize, blockSize);

        final int writeThread = 2;
        final List<Thread> writeThreads = new ArrayList<>();
        final byte[] data = new byte[1024];
        final Random random = new Random();
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) random.nextInt();
        }
        for (int i = 0; i < writeThread; i++) {
            final Thread thread =
                    new Thread(
                            () -> {
                                for (int j = 0; j < dataSize; j++) {
                                    try {
                                        pullQueue.append(0, data);
                                    } catch (IOException ioException) {
                                        ioException.printStackTrace();
                                    }
                                }
                            });
            writeThreads.add(thread);
        }

        List<Thread> readTreads = new ArrayList<>();
        for (String groupId : consumerGroups) {
            final Thread readTread =
                    new Thread(
                            () -> {
                                BlockFileOffset fileOffset = pullQueue.getFileOffset(groupId, 0);
                                DataResultSet result;
                                int readSize = 0;
                                while (readSize < dataSize * writeThread) {
                                    try {
                                        result = pullQueue.take(0, fileOffset);
                                        while (result.hasNext()) {
                                            result.next();
                                            readSize++;
                                        }
                                        fileOffset = result.nexBlockOffset();
                                    } catch (IOException | InterruptedException ioException) {
                                        ioException.printStackTrace();
                                    }
                                }
                                try {
                                    pullQueue.commitFileOffset(groupId, 0, fileOffset);
                                } catch (IOException ioException) {
                                    ioException.printStackTrace();
                                }
                            });
            readTreads.add(readTread);
        }
        writeThreads.forEach(Thread::start);
        readTreads.forEach(Thread::start);
        writeThreads.forEach(FilePullQueueTest::join);
        pullQueue.flush();
        readTreads.forEach(FilePullQueueTest::join);
        pullQueue.close();
    }

    @Test
    public void testOnePartitionMultiWriter() throws IOException {
        for (int j = 0; j < 10; j++) {
            final String dir = "/tmp/test/test_log_6/partition-";
            final int blockSize = 32 * 1024;
            final long segmentSize = 1024L * 1024L * 51;
            final int partitionCount = 1;
            final String consumerGroup = "consumer_group";
            final int maxRecordLength = 1024;
            final PullQueue pullQueue =
                    FilePullQueueTest.createQueue(
                            "event",
                            Collections.singletonList(consumerGroup),
                            partitionCount,
                            dir,
                            segmentSize,
                            blockSize);

            final long perThreadWriteCount = 100000;
            final int writeThread = 15;
            List<Thread> sourceThreads = new ArrayList<>();
            for (int i = 0; i < writeThread; i++) {
                SourceThread sourceThread =
                        new SourceThread(pullQueue, 0, perThreadWriteCount, maxRecordLength, true);
                sourceThreads.add(sourceThread);
            }
            SinkGroup sinkGroup =
                    new SinkGroup(1, pullQueue, consumerGroup, writeThread * perThreadWriteCount);
            for (Thread thread : sourceThreads) {
                thread.start();
            }
            sinkGroup.start();

            sourceThreads.forEach(FilePullQueueTest::join);
            pullQueue.flush();
            FilePullQueueTest.join(sinkGroup);
            pullQueue.close();
            Assert.assertEquals(perThreadWriteCount * writeThread, sinkGroup.getConsumerCount());
        }
    }

    private static void test(
            String dir,
            int partitionCount,
            long dataSize,
            int sinkGroups,
            int blockSize,
            long segmentSize,
            int maxRecordLength,
            boolean random)
            throws IOException {

        final long totalSize = dataSize * partitionCount;
        List<String> consumerGroup = new ArrayList<>(sinkGroups);
        for (int i = 0; i < sinkGroups; i++) {
            consumerGroup.add("consumer_group_" + i);
        }

        final PullQueue pullQueue =
                createQueue("voqa", consumerGroup, partitionCount, dir, segmentSize, blockSize);

        // create sources
        final List<Thread> sourceThread = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            Thread t = new SourceThread(pullQueue, i, dataSize, maxRecordLength, random);
            sourceThread.add(t);
        }

        // create multi sink
        final List<SinkGroup> sinGroup =
                consumerGroup.stream()
                        .map(
                                groupName ->
                                        new SinkGroup(
                                                partitionCount, pullQueue, groupName, totalSize))
                        .collect(Collectors.toList());

        long start = System.currentTimeMillis();
        // start source and sink threads
        sourceThread.forEach(Thread::start);
        sinGroup.forEach(Thread::start);

        // waiting source threads finish and flush
        sourceThread.forEach(FilePullQueueTest::join);
        pullQueue.flush();
        long writeSpend = System.currentTimeMillis() - start;

        // waiting sink threads finish.
        sinGroup.forEach(FilePullQueueTest::join);
        Assert.assertEquals(totalSize, dataSize * partitionCount);
        Assert.assertEquals(
                sinGroup.stream().mapToLong(SinkGroup::getConsumerCount).sum(),
                dataSize * partitionCount * sinkGroups);
        LOG.info(
                "write spend:"
                        + writeSpend
                        + "(ms),write count:"
                        + totalSize
                        + ",read spend:"
                        + (System.currentTimeMillis() - start)
                        + "(ms),read count:"
                        + sinGroup.stream().mapToLong(SinkGroup::getConsumerCount).sum()
                        + ".");
        pullQueue.close();
    }

    public static PullQueue createQueue(
            String topic,
            List<String> consumerGroup,
            int partitionCount,
            String dir,
            long segmentSize,
            int blockSize) {
        return createQueue(
                topic,
                consumerGroup,
                partitionCount,
                dir,
                segmentSize,
                blockSize,
                7,
                TimeUnit.SECONDS,
                SegmentBuilder.CompressionType.ZSTD);
    }

    public static PullQueue createQueue(
            String topic,
            List<String> consumerGroup,
            int partitionCount,
            String dir,
            long segmentSize,
            int blockSize,
            int cleanUpPeriod,
            TimeUnit cleanUpTimeUnit,
            SegmentBuilder.CompressionType compressionType) {
        final List<File> dirs = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            dirs.add(new File(dir + i));
        }
        final PartitionFilePullQueueBuilder builder = PartitionFilePullQueueBuilder.newBuilder();
        builder.segmentSize(segmentSize)
                .blockSize(blockSize)
                .consumerGroups(consumerGroup)
                .topic(topic)
                .cleanUpPeriod(cleanUpPeriod, cleanUpTimeUnit)
                .compressionType(compressionType)
                .flushPeriod(500, TimeUnit.MILLISECONDS);
        return builder.dirs(dirs).build();
    }

    public static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
