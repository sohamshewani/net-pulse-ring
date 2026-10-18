package netpulse.ring;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive unit, invariant, and concurrent stress-test suite for 'net-pulse-ring'.
 * Implements a high-performance ring buffer and pulse coordinator matching the target domain.
 */
public class NetPulseRingTest {

    // =========================================================================
    // Domain Implementation Under Test (Inlined for self-contained compilation)
    // =========================================================================

    public static class PulseEvent {
        public long sequenceId;
        public long timestamp;
        public byte[] payload;
        public boolean isCorrupted;

        public PulseEvent() {
            this.payload = new byte[0];
        }

        public void reset() {
            this.sequenceId = -1L;
            this.timestamp = 0L;
            this.isCorrupted = false;
            if (this.payload.length > 0) {
                Arrays.fill(this.payload, (byte) 0);
            }
        }
    }

    public static class NetPulseRingBuffer {
        private final int capacity;
        private final int mask;
        private final PulseEvent[] entries;
        private final AtomicLong head = new AtomicLong(0L);
        private final AtomicLong tail = new AtomicLong(0L);
        private final AtomicBoolean failed = new AtomicBoolean(false);

        public NetPulseRingBuffer(int capacity) {
            if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be a power of 2 greater than 0");
            }
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.entries = new PulseEvent[capacity];
            for (int i = 0; i < capacity; i++) {
                entries[i] = new PulseEvent();
            }
        }

        public int capacity() {
            return capacity;
        }

        public boolean isFailed() {
            return failed.get();
        }

        public void setFailed(boolean state) {
            failed.set(state);
        }

        public boolean tryPublish(long sequenceId, long timestamp, byte[] src, int offset, int length) {
            if (failed.get()) return false;

            long currentTail = tail.get();
            long currentHead = head.get();

            if (currentTail - currentHead >= capacity) {
                return false; // Full
            }

            if (!tail.compareAndSet(currentTail, currentTail + 1)) {
                return false; // Contention failure
            }

            int index = (int) (currentTail & mask);
            PulseEvent event = entries[index];
            event.sequenceId = sequenceId;
            event.timestamp = timestamp;
            if (event.payload.length < length) {
                event.payload = new byte[Math.max(length, 64)];
            }
            System.arraycopy(src, offset, event.payload, 0, length);
            event.isCorrupted = false;
            return true;
        }

        public boolean poll(Consumer<PulseEvent> consumer) {
            long currentHead = head.get();
            long currentTail = tail.get();

            if (currentHead >= currentTail) {
                return false; // Empty
            }

            int index = (int) (currentHead & mask);
            PulseEvent event = entries[index];

            if (event.sequenceId == -1L || event.isCorrupted) {
                return false;
            }

            consumer.accept(event);

            // Advance head safely
            if (!head.compareAndSet(currentHead, currentHead + 1)) {
                return false;
            }
            return true;
        }

        public void clear() {
            head.set(0L);
            tail.set(0L);
            failed.set(false);
            for (PulseEvent entry : entries) {
                entry.reset();
            }
        }

        public long getHead() {
            return head.get();
        }

        public long getTail() {
            return tail.get();
        }
    }

    public static class PulseCoordinator {
        private final NetPulseRingBuffer ringBuffer;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PulseCoordinator-Heartbeat");
            t.setDaemon(true);
            return t;
        });
        private final AtomicLong heartbeatCounter = new AtomicLong(0);
        private final AtomicBoolean running = new AtomicBoolean(false);

        public PulseCoordinator(NetPulseRingBuffer ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        public void start(long periodMs) {
            if (running.compareAndSet(false, true)) {
                scheduler.scheduleAtFixedRate(this::emitHeartbeat, 0, periodMs, TimeUnit.MILLISECONDS);
            }
        }

        public void stop() {
            if (running.compareAndSet(true, false)) {
                scheduler.shutdownNow();
            }
        }

        private void emitHeartbeat() {
            if (!running.get() || ringBuffer.isFailed()) return;
            long seq = heartbeatCounter.incrementAndGet();
            long time = System.currentTimeMillis();
            byte[] data = String.valueOf(seq).getBytes();
            ringBuffer.tryPublish(seq, time, data, 0, data.length);
        }

        public boolean isRunning() {
            return running.get();
        }
    }

    // =========================================================================
    // Test Suite
    // =========================================================================

    @Nested
    @DisplayName("1. Unit & Boundary Tests")
    class UnitAndBoundaryTests {

        @Test
        @DisplayName("Initialization with invalid capacities throws exception")
        void testInvalidCapacities() {
            assertThrows(IllegalArgumentException.class, () -> new NetPulseRingBuffer(0));
            assertThrows(IllegalArgumentException.class, () -> new NetPulseRingBuffer(-4));
            assertThrows(IllegalArgumentException.class, () -> new NetPulseRingBuffer(3));
            assertThrows(IllegalArgumentException.class, () -> new NetPulseRingBuffer(1000));
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 4, 8, 16, 1024, 65536})
        @DisplayName("Valid power-of-two capacities initialize correctly")
        void testValidCapacities(int capacity) {
            NetPulseRingBuffer ring = new NetPulseRingBuffer(capacity);
            assertEquals(capacity, ring.capacity());
            assertEquals(0L, ring.getHead());
            assertEquals(0L, ring.getTail());
            assertFalse(ring.isFailed());
        }

        @Test
        @DisplayName("Single publish and poll cycle validates payload integrity")
        void testSinglePublishAndPoll() {
            NetPulseRingBuffer ring = new NetPulseRingBuffer(4);
            byte[] payload = "NET-PULSE-DATA".getBytes();

            boolean published = ring.tryPublish(101L, 123456789L, payload, 0, payload.length);
            assertTrue(published);
            assertEquals(0L, ring.getHead());
            assertEquals(1L, ring.getTail());

            List<PulseEvent> polledEvents = new ArrayList<>();
            boolean polled = ring.poll(polledEvents::add);

            assertTrue(polled);
            assertEquals(1, polledEvents.size());
            PulseEvent event = polledEvents.get(0);
            assertEquals(101L, event.sequenceId);
            assertEquals(123456789L, event.timestamp);
            assertArrayEquals(payload, Arrays.copyOf(event.payload, payload.length));
            assertEquals(1L, ring.getHead());
            assertEquals(1L, ring.getTail());
        }

        @Test
        @DisplayName("Ring buffer full condition blocks publishing appropriately")
        void testRingBufferFullCondition() {
            NetPulseRingBuffer ring = new NetPulseRingBuffer(2);
            byte[] data = {1, 2, 3};

            assertTrue(ring.tryPublish(1L, 100L, data, 0, data.length));
            assertTrue(ring.tryPublish(2L, 101L, data, 0, data.length));
            // Buffer is now capacity 2 (Tail=2, Head=0)
            assertFalse(ring.tryPublish(3L, 102L, data, 0, data.length));

            // Consume one
            List<PulseEvent> consumed = new ArrayList<>();
            assertTrue(ring.poll(consumed::add));

            // Now should accept one more
            assertTrue(ring.tryPublish(3L, 102L, data, 0, data.length));
            assertFalse(ring.tryPublish(4L, 103L, data, 0, data.length));
        }

        @Test
        @DisplayName("Polling an empty ring buffer returns false safely")
        void testEmptyPoll() {
            NetPulseRingBuffer ring = new NetPulseRingBuffer(4);
            assertFalse(ring.poll(e -> fail("Should not consume")));
        }
    }

    @Nested
    @DisplayName("2. Invariant & State Verification Tests")
    class InvariantTests {

        @Test
        @DisplayName("Clear resets ring state completely")
        void testRingClearInvariant() {
            NetPulseRingBuffer ring = new NetPulseRingBuffer(4);
            byte[] data = "test".getBytes();
            ring.tryPublish(1L, System.currentTimeMillis(), data, 0, data.length);
            ring.tryPublish(2L, System.currentTimeMillis(), data, 0, data.length);

            ring.clear();

            assertEquals(0L, ring.getHead());
            assertEquals(0L, ring.getTail());
            assertFalse(ring.isFailed());
            assertFalse(ring.poll(e -> fail()));
        }

        @Test
        @DisplayName("Failure state halts publishing operations")
        void testFailureStateIsolation() {
            NetPulseRingBuffer ring = new NetPulseRingBuffer(4);
            byte[] data = "fail-test".getBytes();

            ring.setFailed(true);
            assertTrue(ring.isFailed());

            assertFalse(ring.tryPublish(1L, 1L, data, 0, data.length));
            assertEquals(0L, ring.getTail());

            ring.setFailed(false);
            assertFalse(ring.isFailed());
            assertTrue(ring.tryPublish(1L, 1L, data, 0, data.length));
        }
    }

    @Nested
    @DisplayName("3. Concurrent Stress & Race Condition Tests")
    class ConcurrentStressTests {

        @Test
        @DisplayName("Multi-threaded producer/consumer ring contention stress test")
        void testConcurrentProducerConsumerStress() throws InterruptedException {
            int capacity = 1024;
            NetPulseRingBuffer ring = new NetPulseRingBuffer(capacity);
            int totalMessagesPerProducer = 10_000;
            int producerCount = 4;
            int consumerCount = 4;

            ExecutorService executor = Executors.newFixedThreadPool(producerCount + consumerCount);
            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch endGate = new CountDownLatch(producerCount + consumerCount);

            AtomicLong totalPublished = new AtomicLong(0);
            AtomicLong totalConsumed = new AtomicLong(0);
            ConcurrentHashMap<Long, Boolean> consumedSequenceTracker = new ConcurrentHashMap<>();

            // Producers
            for (int p = 0; p < producerCount; p++) {
                final int producerId = p;
                executor.submit(() -> {
                    try {
                        startGate.await();
                        for (int i = 0; i < totalMessagesPerProducer; i++) {
                            long seq = (producerId * 100_000L) + i;
                            byte[] payload = Long.toString(seq).getBytes();
                            boolean success;
                            do {
                                success = ring.tryPublish(seq, System.nanoTime(), payload, 0, payload.length);
                                if (!success