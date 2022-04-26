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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.name.zicat.queue.IOUtils.readFully;
import static org.name.zicat.queue.IOUtils.writeFull;
import static org.name.zicat.queue.SegmentBuilder.realPrefix;

/**
 * DefaultGroupManager use file to persist group and file offset info.
 *
 * <p>default file name : group_id_${groupId}.index.
 */
public class DefaultGroupManager implements GroupManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultGroupManager.class);

    private static final String FILE_PREFIX = "group_id_";
    private static final String FILE_SUFFIX = ".index";

    private final Map<String, FileOffset> cache = new ConcurrentHashMap<>();
    private final Map<String, FileChannel> fileCache = new ConcurrentHashMap<>();
    private final File dir;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(16);
    private final String filePrefix;

    public DefaultGroupManager(File dir, String topic, List<String> groupIds) {
        this.dir = dir;
        this.filePrefix = realPrefix(topic, FILE_PREFIX);
        loadCache(groupIds);
    }

    /**
     * add group ids to cache.
     *
     * @param groupIds groupIds
     */
    private void loadCache(List<String> groupIds) {

        final int cacheExpectedSize = groupIds.size();
        final File[] files = dir.listFiles(file -> isGroupIndexFile(file.getName()));
        final List<String> newGroups = new ArrayList<>(groupIds);
        if (files != null) {
            for (File file : files) {
                try {
                    final String groupId = groupId(file.getName());
                    if (!newGroups.remove(groupId)) {
                        continue;
                    }
                    final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                    final FileChannel fileChannel = randomAccessFile.getChannel();
                    readFully(fileChannel, byteBuffer, 0);
                    byteBuffer.flip();
                    fileCache.put(groupId, fileChannel);
                    cache.put(groupId, FileOffset.parserByteBuffer(byteBuffer));
                    byteBuffer.clear();
                } catch (Exception e) {
                    throw new IllegalStateException("load group index file error", e);
                }
            }
        }
        newGroups.forEach(
                groupId -> {
                    try {
                        commitFileOffset(groupId, new FileOffset(-1, 0));
                    } catch (Exception e) {
                        throw new IllegalStateException("load group index file error", e);
                    }
                });

        if (cache.size() != cacheExpectedSize) {
            throw new IllegalStateException(
                    "cache size must equal groupIds size, expected size = "
                            + cacheExpectedSize
                            + ", real size = "
                            + cache.size());
        }
    }

    /**
     * check name is group id file.
     *
     * @param name name
     * @return true if group index file
     */
    public boolean isGroupIndexFile(String name) {
        return name.startsWith(filePrefix) && name.endsWith(FILE_SUFFIX);
    }

    /**
     * get group id by name.
     *
     * @param name name
     * @return group id
     */
    public String groupId(String name) {
        return name.substring(filePrefix.length(), name.length() - FILE_SUFFIX.length());
    }

    @Override
    public synchronized void commitFileOffset(String groupId, FileOffset fileOffset)
            throws IOException {

        isOpen();
        FileOffset cachedFileOffset = cache.get(groupId);
        if (cachedFileOffset != null && cachedFileOffset.compareTo(fileOffset) == 0) {
            return;
        }
        final FileChannel fileChannel =
                fileCache.computeIfAbsent(
                        groupId,
                        key -> {
                            if (!FilePullQueueBuilder.makeDir(dir)) {
                                throw new IllegalStateException(
                                        "create dir fail, dir " + dir.getPath());
                            }
                            final File file = new File(dir, filePrefix + key + FILE_SUFFIX);
                            try {
                                return new RandomAccessFile(file, "rw").getChannel();
                            } catch (IOException e) {
                                throw new IllegalStateException("create random file error", e);
                            }
                        });
        fileOffset.fillBuffer(byteBuffer);
        fileChannel.position(0);
        writeFull(fileChannel, byteBuffer);
        force(fileChannel, false);
        byteBuffer.clear();
        cache.put(groupId, fileOffset);
    }

    /**
     * flush to page cache.
     *
     * @param fileChannel fileChannel
     * @param metaData metaData
     */
    private void force(FileChannel fileChannel, boolean metaData) {
        try {
            fileChannel.force(metaData);
        } catch (Throwable e) {
            LOG.warn("flush error", e);
        }
    }

    @Override
    public FileOffset getFileOffset(String groupId) {
        isOpen();
        return cache.get(groupId);
    }

    @Override
    public FileOffset getMinFileOffset() {
        isOpen();
        return cache.values().stream().min(FileOffset::compareTo).orElse(null);
    }

    /** check whether closed. */
    private void isOpen() {
        if (closed.get()) {
            throw new IllegalStateException("DefaultGroupManager is closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            fileCache.forEach((k, c) -> force(c, true));
            fileCache.forEach((k, c) -> IOUtils.closeQuietly(c));
            fileCache.clear();
            cache.clear();
        }
    }
}
