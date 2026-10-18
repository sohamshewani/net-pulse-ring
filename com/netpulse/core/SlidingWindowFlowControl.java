package com.netpulse.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public final class SlidingWindowFlowControl {

    private static final VarHandle HEAD_VH;
    private static final VarHandle TAIL_VH;
    private static final VarHandle WINDOW_SIZE_VH;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD_VH = l.findVarHandle(SlidingWindowFlowControl.class, "headSequence", long.class);
            TAIL_VH = l.findVarHandle(SlidingWindowFlowControl.class, "tailSequence", long.class);
            WINDOW_SIZE_VH = l.findVarHandle(SlidingWindowFlowControl.class, "windowSize", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final int windowCapacity;
    private final long[] ackBitVector;
    private final int bitVectorMask;
    private final int bitVectorShift;

    @jdk.internal.vm.annotation.Contended
    private volatile long headSequence;

    @jdk.internal.vm.annotation.Contended
    private volatile long tailSequence;

    @jdk.internal.vm.annotation.Contended
    private volatile int windowSize;

    private final LongAdder droppedPacketsCounter;
    private final LongAdder duplicatePacketsCounter;
    private final LongAdder outOfOrderPacketsCounter;

    public SlidingWindowFlowControl(int windowCapacity) {
        if (windowCapacity <= 0 || (windowCapacity & (windowCapacity - 1)) != 0) {
            throw new IllegalArgumentException("Window capacity must be a strict power of 2 and greater than 0.");
        }
        this.windowCapacity = windowCapacity;
        this.windowSize = windowCapacity;
        this.headSequence = 0L;
        this.tailSequence = 0L;

        int numWords = Math.max(1, windowCapacity / 64);
        if ((numWords & (numWords - 1)) != 0) {
            int powerOfTwo = 1;
            while (powerOfTwo < numWords) {
                powerOfTwo <<= 1;
            }
            numWords = powerOfTwo;
        }
        this.ackBitVector = new long[numWords];
        this.bitVectorMask = numWords - 1;
        this.bitVectorShift = Integer.numberOfTrailingZeros(64);

        this.droppedPacketsCounter = new LongAdder();
        this.duplicatePacketsCounter = new LongAdder();
        this.outOfOrderPacketsCounter = new LongAdder();
    }

    public boolean tryAcquireSendSlot(long sequenceNumber) {
        long currentTail = (long) TAIL_VH.getAcquire(this);
        long currentHead = (long) HEAD_VH.getAcquire(this);
        int currentWindow = (int) WINDOW_SIZE_VH.getAcquire(this);

        if (currentWindow <= 0) {
            droppedPacketsCounter.increment();
            return false;
        }

        if (sequenceNumber - currentHead >= windowCapacity) {
            droppedPacketsCounter.increment();
            setWindowSize(0);
            return false;
        }

        if (sequenceNumber - currentTail >= currentWindow) {
            droppedPacketsCounter.increment();
            return false;
        }

        return true;
    }

    public FlowControlResult processIncomingPacket(long sequenceNumber) {
        long currentHead = (long) HEAD_VH.getAcquire(this);
        
        long lowerBound = currentHead;
        long upperBound = currentHead + windowCapacity;

        if (sequenceNumber < lowerBound) {
            duplicatePacketsCounter.increment();
            return FlowControlResult.DUPLICATE;
        }

        if (sequenceNumber >= upperBound) {
            outOfOrderPacketsCounter.increment();
            return FlowControlResult.OUT_OF_BOUNDS_DROPPED;
        }

        int bitIndex = (int) sequenceNumber;
        int wordIndex = (bitIndex >>> bitVectorShift) & bitVectorMask;
        long bitMask = 1L << (bitIndex & 63);

        long currentWord = ackBitVector[wordIndex];
        if ((currentWord & bitMask) != 0L) {
            duplicatePacketsCounter.increment();
            return FlowControlResult.DUPLICATE;
        }

        ackBitVector[wordIndex] = currentWord | bitMask;

        advanceWindowIfPossible();

        return FlowControlResult.ACCEPTED;
    }

    private void advanceWindowIfPossible() {
        long currentHead = (long) HEAD_VH.getAcquire(this);
        long newHead = currentHead;

        while (true) {
            int bitIndex = (int) newHead;
            int wordIndex = (bitIndex >>> bitVectorShift) & bitVectorMask;
            long bitMask = 1L << (bitIndex & 63);

            if ((ackBitVector[wordIndex] & bitMask) == 0L) {
                break;
            }

            ackBitVector[wordIndex] &= ~bitMask;
            newHead++;
        }

        if (newHead != currentHead) {
            HEAD_VH.setRelease(this, newHead);
            
            long currentTail = (long) TAIL_VH.getAcquire(this);
            if (newHead > currentTail) {
                TAIL_VH.setRelease(this, newHead);
            }

            int availableWindow = (int) (windowCapacity - (newHead - (long) TAIL_VH.getAcquire(this)));
            if (availableWindow < 0) {
                availableWindow = 0;
            }
            setWindowSize(Math.max(1, availableWindow));
        }
    }

    public void setWindowSize(int newSize) {
        int clamped = Math.max(0, Math.min(newSize, windowCapacity));
        WINDOW_SIZE_VH.setRelease(this, clamped);
    }

    public int getWindowSize() {
        return (int) WINDOW_SIZE_VH.getAcquire(this);
    }

    public long getHeadSequence() {
        return (long) HEAD_VH.getAcquire(this);
    }

    public long getTailSequence() {
        return (long) TAIL_VH.getAcquire(this);
    }

    public long getDroppedPacketsCount() {
        return droppedPacketsCounter.sum();
    }

    public long getDuplicatePacketsCount() {
        return duplicatePacketsCounter.sum();
    }

    public long getOutOfOrderPacketsCount() {
        return outOfOrderPacketsCounter.sum();
    }

    public enum FlowControlResult {
        ACCEPTED,
        DUPLICATE,
        OUT_OF_BOUNDS_DROPPED
    }
}