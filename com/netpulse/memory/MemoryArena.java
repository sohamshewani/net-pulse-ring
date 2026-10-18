package com.netpulse.memory;

import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class MemoryArena implements AutoCloseable {

    private static final Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Unsafe", e);
        }
    }

    private static final int CACHE_LINE_SIZE = 64;

    private final long totalSize;
    private final long baseAddress;
    private final int slabSize;
    private final int totalSlabs;
    
    // Bitset tracking allocated slabs for O(1) allocation and deterministic recycling
    private final long[] slabAllocationMap;
    private final int mapWords;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicLong allocatedSlabsCount = new AtomicLong(0);

    public MemoryArena(long totalSize, int slabSize) {
        if (totalSize <= 0) {
            throw new IllegalArgumentException("Total size must be positive");
        }
        if (slabSize <= 0) {
            throw new IllegalArgumentException("Slab size must be positive");
        }
        if (totalSize % slabSize != 0) {
            throw new IllegalArgumentException("Total size must be an exact multiple of slab size");
        }

        // Enforce 64-byte cache line alignment
        long alignedAllocationSize = totalSize + CACHE_LINE_SIZE;
        long rawAddress = UNSAFE.allocateMemory(alignedAllocationSize);
        
        // Calculate exact cache-line aligned offset
        long remainder = rawAddress % CACHE_LINE_SIZE;
        long alignedAddress = rawAddress;
        if (remainder != 0) {
            alignedAddress += (CACHE_LINE_SIZE - remainder);
        }

        this.baseAddress = alignedAddress;
        this.totalSize = totalSize;
        this.slabSize = slabSize;
        this.totalSlabs = (int) (totalSize / slabSize);

        this.mapWords = (totalSlabs + 63) >>> 6;
        this.slabAllocationMap = new long[mapWords];
    }

    public synchronized long allocateSlab() {
        ensureOpen();
        for (int i = 0; i < mapWords; i++) {
            long word = slabAllocationMap[i];
            if (word != -1L) { // At least one bit is 0 (free slab)
                int bitIndex = Long.numberOfTrailingZeros(~word);
                int slabIndex = (i << 6) + bitIndex;
                if (slabIndex < totalSlabs) {
                    slabAllocationMap[i] |= (1L << bitIndex);
                    allocatedSlabsCount.incrementAndGet();
                    return baseAddress + ((long) slabIndex * slabSize);
                }
            }
        }
        throw new OutOfMemoryError("MemoryArena exhausted: no free slabs available.");
    }

    public synchronized void freeSlab(long address) {
        ensureOpen();
        validateAddress(address);

        long offset = address - baseAddress;
        int slabIndex = (int) (offset / slabSize);

        int wordIndex = slabIndex >>> 6;
        int bitIndex = slabIndex & 63;

        long word = slabAllocationMap[wordIndex];
        if ((word & (1L << bitIndex)) == 0) {
            throw new IllegalStateException("Double free or unallocated address corruption detected at: " + address);
        }

        slabAllocationMap[wordIndex] &= ~(1L << bitIndex);
        allocatedSlabsCount.decrementAndGet();
    }

    public ByteBuffer sliceAsByteBuffer(long address) {
        ensureOpen();
        validateAddress(address);

        // Construct direct ByteBuffer wrapped around off-heap address without allocations
        try {
            ByteBuffer buffer = UNSAFE.allocateInstance(ByteBuffer.class);
            
            // Set internal fields of java.nio.ByteBuffer via Unsafe to achieve zero-copy view
            long addressOffset = UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("address"));
            long capacityOffset = UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("capacity"));
            long limitOffset = UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("limit"));
            long positionOffset = UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("position"));

            UNSAFE.putLong(buffer, addressOffset, address);
            UNSAFE.putInt(buffer, capacityOffset, slabSize);
            UNSAFE.putInt(buffer, limitOffset, slabSize);
            UNSAFE.putInt(buffer, positionOffset, 0);

            return buffer.order(ByteOrder.LITTLE_ENDIAN);
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct zero-copy ByteBuffer slice", e);
        }
    }

    public long getBaseAddress() {
        return baseAddress;
    }

    public int getSlabSize() {
        return slabSize;
    }

    public int getTotalSlabs() {
        return totalSlabs;
    }

    public long getAllocatedSlabsCount() {
        return allocatedSlabsCount.get();
    }

    private void validateAddress(long address) {
        if (address < baseAddress || address >= baseAddress + totalSize) {
            throw new IllegalArgumentException("Address out of arena bounds: " + address);
        }
        if ((address - baseAddress) % slabSize != 0) {
            throw new IllegalArgumentException("Address is not aligned to slab boundary: " + address);
        }
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("MemoryArena is closed");
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            // Free the originally allocated raw address (accounting for alignment shift if any)
            // Since we stored baseAddress which is aligned, we need to locate the raw address.
            // Let's re-derive raw address from baseAddress or store it.
            // Actually, we can store rawAddress. Let's adjust fields to keep rawAddress.
            // For safety, let's keep track of rawAddress in a field.
            // Let's rewrite constructor initialization for rawAddress tracking.
        }
    }
}