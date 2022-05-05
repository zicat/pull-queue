package org.name.zicat.queue;

import java.nio.ByteBuffer;

import static org.name.zicat.queue.SegmentBuilder.BLOCK_HEAD_SIZE;

/** BlockSet. */
public class BlockSet {

    private ByteBuffer writeBlock;
    private ByteBuffer compressionBlock;
    private ByteBuffer encodeBlock;
    private int blockSize;

    public BlockSet(int blockSize) {
        this.blockSize = blockSize;
        this.writeBlock = ByteBuffer.allocateDirect(blockSize);
        this.compressionBlock = ByteBuffer.allocateDirect(blockSize);
        this.encodeBlock = ByteBuffer.allocateDirect(blockSize + BLOCK_HEAD_SIZE);
    }

    public ByteBuffer writeBlock() {
        return writeBlock;
    }

    public int blockSize() {
        return blockSize;
    }

    public ByteBuffer encodeBlock() {
        return encodeBlock;
    }

    public ByteBuffer compressionBlock() {
        return compressionBlock;
    }

    public ByteBuffer compressionBlock(ByteBuffer newBlock) {
        this.compressionBlock = newBlock;
        return newBlock;
    }

    public ByteBuffer encodeBlock(ByteBuffer newBlock) {
        this.encodeBlock = newBlock;
        return newBlock;
    }

    public BlockSet reAllocate(int blockSize) {
        this.blockSize = blockSize;
        this.writeBlock = IOUtils.reAllocate(writeBlock, blockSize);
        this.compressionBlock = IOUtils.reAllocate(compressionBlock, blockSize);
        this.encodeBlock = IOUtils.reAllocate(encodeBlock, blockSize + BLOCK_HEAD_SIZE);
        return this;
    }
}
