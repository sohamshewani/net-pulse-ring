package com.netpulse.types;

import jdk.internal.misc.Contended;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public final class PacketBuffer implements AutoCloseable {

    private static final long CACHE_LINE_SIZE = 64L;
    
    // Layout offsets within the metadata preamble of a packet buffer slice
    private static final long OFFSET_MAGIC       = 0;  // int (4 bytes)
    private static final long OFFSET_FLAGS       = 4;  // int (4 bytes)
    private static final long OFFSET_SEQUENCE    = 8;  // long (8 bytes)
    private static final long OFFSET_TIMESTAMP   = 16; // long (8 bytes)
    private static final long OFFSET_LENGTH      = 24; // int (4 bytes)
    private static final long OFFSET_CAPACITY    = 28; // int (4 bytes)
    private static final long METADATA_SIZE      = 32; // 32 bytes aligned header

    public static final int MAGIC_VAL = 0x4E50554C; // "NPUL"

    private final MemorySegment segment;
    private final boolean direct;
    
    @Contended
    private volatile int refCount;
    private static final AtomicIntegerFieldUpdater<PacketBuffer> REF_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PacketBuffer.class, "refCount");

    private final ArenaAllocator ownerArena;
    private final long arenaOffset;

    public PacketBuffer(MemorySegment segment, ArenaAllocator ownerArena, long arenaOffset, boolean direct) {
        this.segment = Objects.requireNonNull(segment, "segment cannot be null");
        this.ownerArena = ownerArena;
        this.arenaOffset = arenaOffset;
        this.direct = direct;
        this.refCount = 1;
        
        // Initialize magic marker
        this.segment.set(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, OFFSET_MAGIC, MAGIC_VAL);
    }

    public static PacketBuffer allocateHeap(int capacity) {
        int totalSize = (int) (METADATA_SIZE + capacity);
        MemorySegment seg = MemorySegment.ofArray(new byte[totalSize]);
        PacketBuffer buf = new PacketBuffer(seg, null, -1L, false);
        buf.setCapacity(capacity);
        buf.setLength(0);
        buf.setFlags(0);
        return buf;
    }

    public static PacketBuffer allocateDirectAligned(int capacity) {
        int totalSize = (int) (METADATA_SIZE + capacity);
        // Align to 64 bytes
        long alignedSize = (totalSize + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
        MemorySegment seg = MemorySegment.allocateNative(alignedSize, CACHE_LINE_SIZE, java.lang.foreign.Arena.global());
        PacketBuffer buf = new PacketBuffer(seg, null, -1L, true);
        buf.setCapacity(capacity);
        buf.setLength(0);
        buf.setFlags(0);
        return buf;
    }

    public MemorySegment segment() {
        ensureValid();
        return segment;
    }

    public int capacity() {
        return segment.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, OFFSET_CAPACITY);
    }

    private void setCapacity(int capacity) {
        segment.set(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, OFFSET_CAPACITY, capacity);
    }

    public int length() {
        return segment.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, OFFSET_LENGTH);
    }

    public void setLength(int length) {
        int cap = capacity();
        if (length < 0 || length > cap) {
            throw new IllegalArgumentException("Length out of bounds: " + length + ", capacity: " + cap);
        }
        segment.set(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, OFFSET_LENGTH, length);
    }

    public long sequence() {
        return segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_SEQUENCE);
    }

    public void setSequence(long sequence) {
        segment.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_SEQUENCE, sequence);
    }

    public long timestamp() {
        return segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_TIMESTAMP);
    }

    public void setTimestamp(long timestamp) {
        segment.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_TIMESTAMP, timestamp);
    }

    public int flags() {
        return segment.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, OFFSET_FLAGS);
    }

    public void setFlags(int flags) {
        segment.set(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, OFFSET_FLAGS, flags);
    }

    public void writeBytes(int destinationOffset, byte[] src, int srcOffset, int length) {
        ensureValid();
        if (destinationOffset < 0 || length < 0 || destinationOffset + length > capacity()) {
            throw new IndexOutOfBoundsException("Write out of bounds");
        }
        MemorySegment.copy(src, srcOffset, segment, java.lang.foreign.ValueLayout.JAVA_BYTE, METADATA_SIZE + destinationOffset, length);
    }

    public void readBytes(int sourceOffset, byte[] dst, int dstOffset, int length) {
        ensureValid();
        if (sourceOffset < 0 || length < 0 || sourceOffset + length > length()) {
            throw new IndexOutOfBoundsException("Read out of bounds");
        }
        MemorySegment.copy(segment, java.lang.foreign.ValueLayout.JAVA_BYTE, METADATA_SIZE + sourceOffset, dst, dstOffset, length);
    }

    public void writeByteBuffer(int destinationOffset, ByteBuffer src) {
        ensureValid();
        int remaining = src.remaining();
        if (destinationOffset < 0 || destinationOffset + remaining > capacity()) {
            throw new IndexOutOfBoundsException("ByteBuffer write out of bounds");
        }
        MemorySegment targetSlice = segment.asSlice(METADATA_SIZE + destinationOffset, remaining);
        if (src.isDirect()) {
            MemorySegment srcSegment = MemorySegment.ofBuffer(src);
            targetSlice.copyFrom(srcSegment);
            src.position(src.limit());
        } else {
            byte[] arr = src.array();
            int offset = src.arrayOffset() + src.position();
            MemorySegment.copy(arr, offset, targetSlice, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, remaining);
            src.position(src.limit());
        }
    }

    public ByteBuffer toByteBuffer() {
        ensureValid();
        return segment.asSlice(METADATA_SIZE, length()).asByteBuffer().order(ByteOrder.BIG_ENDIAN);
    }

    public PacketBuffer retain() {
        while (true) {
            int current = refCount;
            if (current <= 0) {
                throw new IllegalStateException("Cannot retain a released PacketBuffer");
            }
            if (REF_UPDATER.compareAndSet(this, current, current + 1)) {
                return this;
            }
        }
    }

    @Override
    public void close() {
        while (true) {
            int current = refCount;
            if (current <= 0) {
                return; // Already released
            }
            if (REF_UPDATER.compareAndSet(this, current, current - 1)) {
                if (current - 1 == 0) {
                    recycle();
                }
                return;
            }
        }
    }

    private void recycle() {
        if (ownerArena != null) {
            ownerArena.releaseBlock(arenaOffset);
        } else if (direct && segment.address() != 0) {
            // Standalone native allocation cleanup is handled via Arena lifecycle or explicit drop if needed
        }
    }

    private void ensureValid() {
        if (refCount <= 0) {
            throw new IllegalStateException("PacketBuffer is released or invalid");
        }
        int magic = segment.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, OFFSET_MAGIC);
        if (magic != MAGIC_VAL) {
            throw new IllegalStateException("Memory corruption detected: invalid magic number");
        }
    }

    public interface ArenaAllocator {
        void releaseBlock(long offset);
    }
}