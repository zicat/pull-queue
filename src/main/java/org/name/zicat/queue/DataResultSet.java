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

import java.util.Iterator;

import static org.name.zicat.queue.VIntUtil.readVInt;

/**
 * DataResultSet support iterator data from block.
 *
 * @author zicat
 */
public class DataResultSet implements Iterator<byte[]> {

    private final BlockFileOffset blockFileOffset;
    private volatile byte[] nextData;
    private final long readBytes;

    public DataResultSet(BlockFileOffset blockFileOffset, long readBytes) {
        this.blockFileOffset = blockFileOffset;
        this.readBytes = readBytes;
        next();
    }

    public BlockFileOffset nexBlockOffset() {
        return blockFileOffset;
    }

    public long readBytes() {
        return readBytes;
    }

    @Override
    public boolean hasNext() {
        return nextData != null;
    }

    @Override
    public byte[] next() {

        final byte[] result = this.nextData;
        if (blockFileOffset.getDataBuffer() == null
                || !blockFileOffset.getDataBuffer().hasRemaining()) {
            this.nextData = null;
        } else {
            int length = readVInt(blockFileOffset.getDataBuffer());
            byte[] bs = new byte[length];
            blockFileOffset.getDataBuffer().get(bs);
            this.nextData = bs;
        }
        return result;
    }
}
