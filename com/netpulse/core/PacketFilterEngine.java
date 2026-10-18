package com.netpulse.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.Arena;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class PacketFilterEngine implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(PacketFilterEngine.class.getName());
    
    private final int ringCapacity;
    private final LockFreeSpscRing inboundRing;
    private final LockFreeSpscRing filteredRing;
    private final PacketMemoryArena memoryArena;
    private final PacketFilterPredicate filterPredicate;
    private final SlidingWindowFlowController flowController;
    private final AtomicBoolean running;
    private final Thread workerThread;

    public static final int CACHE_LINE_SIZE = 64;
    public static final int PACKET_HEADER_SIZE = 32;

    public PacketFilterEngine(int ringCapacity, long totalMemoryBytes, PacketFilterPredicate filterPredicate) {
        if (ringCapacity <= 0 || (ringCapacity & (ringCapacity - 1)) != 0) {
            throw new IllegalArgumentException("Ring capacity must be a strict power of 2.");
        }
        this.ringCapacity = ringCapacity;
        this.inboundRing = new LockFreeSpscRing(ringCapacity);
        this.filteredRing = new LockFreeSpscRing(ringCapacity);
        this.memoryArena = new PacketMemoryArena(totalMemoryBytes, ringCapacity);
        this.filterPredicate = Objects.requireNonNull(filterPredicate, "Filter predicate cannot be null.");
        this.flowController = new SlidingWindowFlowController(64);
        this.running = new AtomicBoolean(false);
        this.workerThread = new Thread(this::pollLoop, "NetPulse-Worker-1:1");
        this.workerThread.setDaemon(true);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            workerThread.start();
            LOGGER.info("PacketFilterEngine started successfully with strict thread confinement.");
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            try {
                workerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warning("Interrupted while waiting for PacketFilterEngine worker to stop.");
            }
            memoryArena.close();
            LOGGER.info("PacketFilterEngine closed and memory resources reclaimed.");
        }
    }

    public boolean offerInboundPacket(long memoryAddress, int length, long sequenceNumber) {
        if (!running.get()) {
            return false;
        }

        if (!flowController.validateAndTrack(sequenceNumber)) {
            LOGGER.log(Level.FINE, "Packet dropped due to sliding window flow control validation failure. Seq: {0}", sequenceNumber);
            return false;
        }

        PacketDescriptor descriptor = new PacketDescriptor(memoryAddress, length, sequenceNumber);
        int spins = 0;
        while (!inboundRing.tryPush(descriptor)) {
            if (!running.get()) {
                return false;
            }
            spins++;
            if (spins > 100) {
                Thread.onSpinWait();
                if (spins > 10000) {
                    LOGGER.warning("Ring Buffer Exhaustion (Backpressure): Dropping packet due to consumer lag.");
                    return false;
                }
            }
        }
        return true;
    }

    public PacketDescriptor pollFilteredPacket() {
        return filteredRing.tryPop();
    }

    private void pollLoop() {
        final int batchSize = 32;
        final PacketDescriptor[] localBatch = new PacketDescriptor[batchSize];

        while (running.get()) {
            int polledCount = inboundRing.tryPopBatch(localBatch, batchSize);
            if (polledCount == 0) {
                Thread.onSpinWait();
                continue;
            }

            for (int i = 0; i < polledCount; i++) {
                PacketDescriptor packet = localBatch[i];
                try {
                    boolean accepted = filterPredicate.test(packet.memoryAddress(), packet.length());
                    if (accepted) {
                        int pushSpins = 0;
                        while (!filteredRing.tryPush(packet)) {
                            if (!running.get()) {
                                break;
                            }
                            pushSpins++;
                            if (pushSpins > 1000) {
                                Thread.onSpinWait();
                            }
                        }
                    } else {
                        memoryArena.recycle(packet.memoryAddress());
                    }
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, "Unhandled exception within filter predicate execution. Isolating corrupt buffer.", t);
                    memoryArena.recycle(packet.memoryAddress());
                }
            }
        }
    }

    public static final class PacketDescriptor {
        private final long memoryAddress;
        private final int length;
        private final long sequenceNumber;

        public PacketDescriptor(long memoryAddress, int length, long sequenceNumber) {
            this.memoryAddress = memoryAddress;
            this.length = length;
            this.sequenceNumber = sequenceNumber;
        }

        public long memoryAddress() {
            return memoryAddress;
        }

        public int length() {
            return length;
        }

        public long sequenceNumber() {
            return sequenceNumber;
        }
    }

    public interface PacketFilterPredicate {
        boolean test(long memoryAddress, int length);
    }

    public static final class LockFreeSpscRing {
        private final int capacity;
        private final int mask;
        private final PacketDescriptor[] buffer;

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

        @jdk.internal.vm.annotation.Contended
        private volatile long head = 0L;

        @jdk.internal.vm.annotation.Contended
        private volatile long tail = 0L;

        public LockFreeSpscRing(int capacity) {
            if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be a strict power of 2.");
            }
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.buffer = new PacketDescriptor[capacity];
        }

        public boolean tryPush(PacketDescriptor descriptor) {
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);

            if (currentTail - currentHead >= capacity) {
                return false;
            }

            buffer[(int) (currentTail & mask)] = descriptor;
            TAIL_HANDLE.setRelease(this, currentTail + 1);
            return true;
        }

        public PacketDescriptor tryPop() {
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);

            if (currentHead >= currentTail) {
                return null;
            }

            int index = (int) (currentHead & mask);
            PacketDescriptor descriptor = buffer[index];
            buffer[index] = null;
            HEAD_HANDLE.setRelease(this, currentHead + 1);
            return descriptor;
        }

        public int tryPopBatch(PacketDescriptor[] dst, int maxCount) {
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);

            long available = currentTail - currentHead;
            if (available <= 0) {
                return 0;
            }

            int fetchCount = (int) Math.min(available, maxCount);
            for (int i = 0; i < fetchCount; i++) {
                long h = currentHead + i;
                int index = (int) (h & mask);
                dst[i] = buffer[index];
                buffer[index] = null;
            }
            HEAD_HANDLE.setRelease(this, currentHead + fetchCount);
            return fetchCount;
        }
    }

    public static final class PacketMemoryArena implements AutoCloseable {
        private final Arena arena;
        private final MemorySegment segment;
        private final long totalSize;
        private final int slabSize;
        private final int totalSlabs;
        private final long baseAddress;
        
        private final LockFreeSpscRing freeSlabsQueue;

        public PacketMemoryArena(long totalSize, int maxSlabs) {
            this.totalSize = totalSize;
            this.totalSlabs = maxSlabs;
            this.slabSize = (int) (totalSize / maxSlabs);
            if (this.slabSize < CACHE_LINE_SIZE) {
                throw new IllegalArgumentException("Slab size must be at least 64 bytes for cache line alignment.");
            }
            
            this.arena = Arena.ofShared();
            this.segment = arena.allocate(totalSize, CACHE_LINE_SIZE);
            this.baseAddress = segment.address();
            this.freeSlabsQueue = new LockFreeSpscRing(maxSlabs);

            for (int i = 0; i < totalSlabs; i++) {
                long slabAddress = baseAddress + ((long) i * slabSize);
                PacketDescriptor desc = new PacketDescriptor(slabAddress, slabSize, 0L);
                boolean pushed = freeSlabsQueue.tryPush(desc);
                if (!pushed) {
                    throw new IllegalStateException("Failed to initialize memory arena slabs.");
                }
            }
        }

        public long allocate() {
            PacketDescriptor desc = freeSlabsQueue.tryPop();
            if (desc == null) {
                LOGGER.severe("Memory Arena Exhaustion: Triggering asynchronous graceful degradation.");
                return 0L;
            }
            return desc.memoryAddress();
        }

        public void recycle(long memoryAddress) {
            PacketDescriptor desc = new PacketDescriptor(memoryAddress, slabSize, 0L);
            while (!freeSlabsQueue.tryPush(desc)) {
                Thread.onSpinWait();
            }
        }

        @Override
        public void close() {
            arena.close();
        }
    }

    public static final class SlidingWindowFlowController {
        private final int windowSize;
        private long expectedSequenceNumber;
        private long bitVector;

        public SlidingWindowFlowController(int windowSize) {
            if (windowSize <= 0 || windowSize > 64) {
                throw new IllegalArgumentException("Window size must be between 1 and 64.");
            }
            this.windowSize = windowSize;
            this.expectedSequenceNumber = 0L;
            this.bitVector = 0L;
        }

        public synchronized boolean validateAndTrack(long sequenceNumber) {
            if (sequenceNumber < expectedSequenceNumber) {
                long diff = expectedSequenceNumber - sequenceNumber;
                if (diff < windowSize) {
                    long bit = 1L << (diff - 1);
                    if ((bitVector & bit) != 0) {
                        return false; // Duplicate
                    }
                    bitVector |= bit;
                    return true;
                }
                return false; // Too old, outside window
            }

            long diff = sequenceNumber - expectedSequenceNumber;
            if (diff >= windowSize) {
                return false; // Out of bounds, flow control window exceeded
            }

            if (diff == 0) {
                expectedSequenceNumber++;
                bitVector <<= 1;
                return true;
            } else {
                long bit = 1L << (diff - 1);
                if ((bitVector & bit) != 0) {
                    return false; // Duplicate
                }
                bitVector |= bit;
                return true;
            }
        }
    }
}