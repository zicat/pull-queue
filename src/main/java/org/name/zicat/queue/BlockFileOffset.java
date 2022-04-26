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

import java.nio.ByteBuffer;

/**
 * The BlockFileOffset maintain the block data of the File Offset.
 *
 * @see FileOffset
 */
public class BlockFileOffset extends FileOffset {

    private final ByteBuffer dataBuffer;
    private final ByteBuffer reusedDataBodyBuffer;
    private final ByteBuffer reusedDataLengthBuffer;

    public BlockFileOffset(FileOffset fileOffset) {
        this(fileOffset.fileId(), fileOffset.offset(), null, null, null);
    }

    public BlockFileOffset(long fileId, long offset) {
        this(fileId, offset, null, null, null);
    }

    public BlockFileOffset(
            long fileId,
            long offset,
            ByteBuffer dataBuffer,
            ByteBuffer reusedDataBodyBuffer,
            ByteBuffer reusedDataLengthBuffer) {
        super(fileId, offset);
        this.dataBuffer = dataBuffer;
        this.reusedDataBodyBuffer = reusedDataBodyBuffer;
        this.reusedDataLengthBuffer = reusedDataLengthBuffer;
    }

    public ByteBuffer getDataBuffer() {
        return dataBuffer;
    }

    public ByteBuffer getReusedDataBodyBuffer() {
        return reusedDataBodyBuffer;
    }

    public ByteBuffer getReusedDataLengthBuffer() {
        return reusedDataLengthBuffer;
    }
}
