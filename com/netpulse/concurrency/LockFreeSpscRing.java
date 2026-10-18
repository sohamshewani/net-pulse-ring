package com.netpulse.concurrency;

import jdk.internal.vm.annotation.Contended;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;

public final class LockFreeSpscRing {

    private final int capacity;
    private final int mask;
    private final ByteBuffer backingBuffer;
    private final long baseAddress;
    private final int elementShift;

    @Contended
    private long head;

    @Contended
    private long tail;

    private static final VarHandle HEAD_HANDLE;
    private static final VarHandle TAIL_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD_HANDLE = l.findVarHandle(LockFreeSpscRing.class, "head", long.class);
            TAIL_HANDLE = l.findVarHandle(LockFreeSpscRing.class, "tail", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public LockFreeSpscRing(int capacity, int elementSize) {
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("Capacity must be a strict power of 2: " + capacity);
        }
        if (elementSize <= 0) {
            throw new IllegalArgumentException("Element size must be greater than 0: " + elementSize);
        }

        this.capacity = capacity;
        this.mask = capacity - 1;
        
        int alignedElementSize = (elementSize + 63) & ~63;
        long totalSize = (long) capacity * alignedElementSize;
        
        this.elementShift = Long.numberOfTrailingZeros(alignedElementSize);
        this.backingBuffer = ByteBuffer.allocateDirect((int) totalSize + 128);
        
        long address;
        try {
            java.lang.reflect.Field addressField = java.nio.Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            long rawAddress = (long) addressField.get(backingBuffer);
            address = (rawAddress + 63L) & ~63L;
        } catch (Exception e) {
            address = 0L;
            throw new RuntimeException("Failed to resolve direct buffer memory address via Unsafe reflection", e);
        }
        this.baseAddress = address;
        
        this.head = 0L;
        this.tail = 0L;
    }

    public boolean offer(ByteBuffer src, int length) {
        long currentHead = (long) HEAD_HANDLE.getAcquire(this);
        long currentTail = (long) TAIL_HANDLE.getAcquire(this);

        if (currentHead - currentTail >= capacity) {
            return false;
        }

        long targetIndex = currentHead;
        long offset = baseAddress + ((targetIndex & mask) << elementShift);

        ByteBuffer slice = backingBuffer.duplicate();
        long bufferAddress = getDirectBufferAddress(slice);
        long targetMemoryAddress = offset;

        long remaining = length;
        int srcPosition = src.position();
        
        while (remaining > 0) {
            long chunk = Math.min(remaining, 8L);
            long value = 0L;
            for (int i = 0; i < chunk; i++) {
                value |= ((long) (src.get(srcPosition + (int)i) & 0xFF)) << (i * 8);
            }
            unsafePutLong(targetMemoryAddress, value);
            targetMemoryAddress += chunk;
            srcPosition += (int) chunk;
            remaining -= chunk;
        }

        HEAD_HANDLE.setRelease(this, currentHead + 1);
        return true;
    }

    public boolean poll(ByteBuffer dst) {
        long currentTail = (long) TAIL_HANDLE.getAcquire(this);
        long currentHead = (long) HEAD_HANDLE.getAcquire(this);

        if (currentTail == currentHead) {
            return false;
        }

        long targetIndex = currentTail;
        long offset = baseAddress + ((targetIndex & mask) << elementShift);

        long targetMemoryAddress = offset;
        int dstPosition = dst.position();
        long remaining = 64L; // Default standard block read size

        while (remaining > 0) {
            long value = unsafeGetLong(targetMemoryAddress);
            for (int i = 0; i < 8 && i < remaining; i++) {
                dst.put(dstPosition + i, (byte) ((value >>> (i * 8)) & 0xFF));
            }
            targetMemoryAddress += 8;
            dstPosition += 8;
            remaining -= 8;
        }

        TAIL_HANDLE.setRelease(this, currentTail + 1);
        return true;
    }

    public int size() {
        long currentHead = (long) HEAD_HANDLE.getAcquire(this);
        long currentTail = (long) TAIL_HANDLE.getAcquire(this);
        return (int) (currentHead - currentTail);
    }

    public boolean isEmpty() {
        return (long) HEAD_HANDLE.getAcquire(this) == (long) TAIL_HANDLE.getAcquire(this);
    }

    public boolean isFull() {
        long currentHead = (long) HEAD_HANDLE.getAcquire(this);
        long currentTail = (long) TAIL_HANDLE.getAcquire(this);
        return (currentHead - currentTail) >= capacity;
    }

    public int capacity() {
        return this.capacity;
    }

    private static long getDirectBufferAddress(ByteBuffer buffer) {
        try {
            java.lang.reflect.Field addressField = java.nio.Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            return (long) addressField.get(buffer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void unsafePutLong(long address, long value) {
        sun.misc.Unsafe unsafe = getUnsafe();
        unsafe.putLong(address, value);
    }

    private static long unsafeGetLong(long address) {
        sun.misc.Unsafe unsafe = getUnsafe();
        return unsafe.getLong(address);
    }

    private static sun.misc.Unsafe getUnsafe() {
        try {
            java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unsafe unavailable", e);
        }
    }
}