package com.netpulse.net;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class NonBlockingPollLoop implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(NonBlockingPollLoop.class.getName());

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final LockFreeSpscRing<DirectPacketBuffer> rxRing;
    private final LockFreeSpscRing<DirectPacketBuffer> txRing;
    private final MemoryArena memoryArena;
    private final AtomicBoolean running;
    private final int workerId;
    
    private static final int DEFAULT_BUFFER_CAPACITY = 2048;

    public NonBlockingPollLoop(int workerId, int ringCapacity, int arenaBlockSize, int blockCount) throws IOException {
        this.workerId = workerId;
        this.running = new AtomicBoolean(false);
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        
        this.rxRing = new LockFreeSpscRing<>(ringCapacity);
        this.txRing = new LockFreeSpscRing<>(ringCapacity);
        this.memoryArena = new MemoryArena(arenaBlockSize, blockCount);
    }

    public void bind(InetSocketAddress address) throws IOException {
        serverChannel.bind(address);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        LOGGER.log(Level.INFO, "Worker {0} bound to {1}", new Object[]{workerId, address});
    }

    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        LOGGER.log(Level.INFO, "Poll loop worker {0} started execution.", workerId);
        
        Thread.currentThread().setName("net-pulse-poll-worker-" + workerId);

        try {
            while (running.get()) {
                int selectedKeys = selector.selectNow();
                if (selectedKeys > 0) {
                    Set<SelectionKey> keys = selector.selectedKeys();
                    Iterator<SelectionKey> iterator = keys.iterator();

                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        iterator.remove();

                        if (!key.isValid()) {
                            continue;
                        }

                        try {
                            if (key.isAcceptable()) {
                                handleAccept(key);
                            } else if (key.isReadable()) {
                                handleRead(key);
                            } else if (key.isWritable()) {
                                handleWrite(key);
                            }
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, "Error processing selection key on worker " + workerId, e);
                            handleKeyFault(key);
                        }
                    }
                }

                drainTxRing();
                Thread.onSpinWait();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Fatal I/O exception in poll loop worker " + workerId, e);
        } finally {
            closeAndCleanup();
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        if (client != null) {
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ);
            LOGGER.log(Level.FINE, "Accepted new connection on worker {0}: {1}", new Object[]{workerId, client.getRemoteAddress()});
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        DirectPacketBuffer packetBuffer = null;

        try {
            packetBuffer = memoryArena.allocate(DEFAULT_BUFFER_CAPACITY);
            if (packetBuffer == null) {
                LOGGER.log(Level.WARNING, "Memory arena exhausted on worker {0}. Shedding load.", workerId);
                key.cancel();
                channel.close();
                return;
            }

            ByteBuffer nativeBuffer = packetBuffer.getNioBuffer();
            nativeBuffer.clear();
            
            int bytesRead = channel.read(nativeBuffer);
            if (bytesRead == -1) {
                closeChannel(key, channel, packetBuffer);
                return;
            }

            if (bytesRead > 0) {
                nativeBuffer.flip();
                packetBuffer.setLength(bytesRead);

                while (!rxRing.tryEnqueue(packetBuffer)) {
                    Thread.onSpinWait();
                }
            } else {
                memoryArena.recycle(packetBuffer);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Fault during read operation on worker " + workerId, e);
            if (packetBuffer != null) {
                memoryArena.recycle(packetBuffer);
            }
            closeKey(key, channel);
        }
    }

    private void handleWrite(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        DirectPacketBuffer packetBuffer = (DirectPacketBuffer) key.attachment();

        if (packetBuffer == null) {
            key.interestOps(SelectionKey.OP_READ);
            return;
        }

        try {
            ByteBuffer nativeBuffer = packetBuffer.getNioBuffer();
            channel.write(nativeBuffer);

            if (!nativeBuffer.hasRemaining()) {
                key.attach(null);
                key.interestOps(SelectionKey.OP_READ);
                memoryArena.recycle(packetBuffer);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Fault during write operation on worker " + workerId, e);
            memoryArena.recycle(packetBuffer);
            closeKey(key, channel);
        }
    }

    private void drainTxRing() {
        DirectPacketBuffer packetBuffer;
        while ((packetBuffer = txRing.tryDequeue()) != null) {
            SocketChannel channel = packetBuffer.getTargetChannel();
            if (channel != null && channel.isOpen()) {
                try {
                    SelectionKey key = channel.keyFor(selector);
                    if (key != null && key.isValid()) {
                        ByteBuffer nativeBuffer = packetBuffer.getNioBuffer();
                        channel.write(nativeBuffer);
                        if (nativeBuffer.hasRemaining()) {
                            key.attach(packetBuffer);
                            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        } else {
                            memoryArena.recycle(packetBuffer);
                        }
                    } else {
                        memoryArena.recycle(packetBuffer);
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Failed to drain TX packet on worker " + workerId, e);
                    memoryArena.recycle(packetBuffer);
                }
            } else {
                memoryArena.recycle(packetBuffer);
            }
        }
    }

    private void handleKeyFault(SelectionKey key) {
        if (key.attachment() instanceof DirectPacketBuffer) {
            memoryArena.recycle((DirectPacketBuffer) key.attachment());
        }
        closeKey(key, key.channel());
    }

    private void closeChannel(SelectionKey key, SocketChannel channel, DirectPacketBuffer buffer) {
        memoryArena.recycle(buffer);
        closeKey(key, channel);
    }

    private void closeKey(SelectionKey key, java.nio.channels.Channel channel) {
        try {
            key.cancel();
            channel.close();
        } catch (IOException e) {
            LOGGER.log(Level.FINE, "Error closing channel", e);
        }
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    private void closeAndCleanup() {
        try {
            selector.close();
            serverChannel.close();
            memoryArena.release();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error during poll loop cleanup on worker " + workerId, e);
        }
    }

    public LockFreeSpscRing<DirectPacketBuffer> getRxRing() {
        return rxRing;
    }

    public LockFreeSpseRing<DirectPacketBuffer> getTxRing() {
        return txRing;
    }

    public static final class DirectPacketBuffer {
        private final ByteBuffer nioBuffer;
        private int length;
        private SocketChannel targetChannel;
        private long sequenceNumber;

        public DirectPacketBuffer(ByteBuffer nioBuffer) {
            this.nioBuffer = nioBuffer;
            this.length = 0;
        }

        public ByteBuffer getNioBuffer() {
            return nioBuffer;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
            this.nioBuffer.limit(length);
        }

        public SocketChannel getTargetChannel() {
            return targetChannel;
        }

        public void setTargetChannel(SocketChannel targetChannel) {
            this.targetChannel = targetChannel;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        public void setSequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        public void reset() {
            this.length = 0;
            this.targetChannel = null;
            this.sequenceNumber = 0L;
            this.nioBuffer.clear();
        }
    }

    public static final class MemoryArena {
        private final ByteBuffer pool;
        private final DirectPacketBuffer[] freeBuffers;
        private int freeIndex;
        private final int blockSize;
        private final int blockCount;

        public MemoryArena(int blockSize, int blockCount) {
            this.blockSize = blockSize;
            this.blockCount = blockCount;
            int totalSize = blockSize * blockCount;
            this.pool = ByteBuffer.allocateDirect(totalSize);
            this.freeBuffers = new DirectPacketBuffer[blockCount];
            this.freeIndex = blockCount;

            for (int i = 0; i < blockCount; i++) {
                int position = i * blockSize;
                pool.position(position);
                pool.limit(position + blockSize);
                ByteBuffer slice = pool.slice();
                freeBuffers[i] = new DirectPacketBuffer(slice);
            }
        }

        public synchronized DirectPacketBuffer allocate(int requestedCapacity) {
            if (requestedCapacity > blockSize) {
                return null;
            }
            if (freeIndex <= 0) {
                return null;
            }
            freeIndex--;
            DirectPacketBuffer buffer = freeBuffers[freeIndex];
            freeBuffers[freeIndex] = null;
            buffer.reset();
            return buffer;
        }

        public synchronized void recycle(DirectPacketBuffer buffer) {
            if (freeIndex < blockCount) {
                buffer.reset();
                freeBuffers[freeIndex] = buffer;
                freeIndex++;
            }
        }

        public void release() {
            
        }
    }

    @SuppressWarnings("unused")
    private static class RingBufferPadding {
        long p0, p1, p2, p3, p4, p5, p6;
    }

    public static final class LockFreeSpscRing<T> extends RingBufferPadding {
        private final int capacity;
        private final int mask;
        private final Object[] buffer;

        private static final VarHandle HEAD_HANDLE;
        private static final VarHandle TAIL_HANDLE;

        @SuppressWarnings("unused")
        private long head = 0L;
        @SuppressWarnings("unused")
        private long tail = 0L;

        static {
            try {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                HEAD_HANDLE = lookup.findVarHandle(LockFreeSpscRing.class, "head", long.class);
                TAIL_HANDLE = lookup.findVarHandle(LockFreeSpscRing.class, "tail", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public LockFreeSpscRing(int capacity) {
            if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be a strict power of 2");
            }
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.buffer = new Object[capacity];
        }

        public boolean tryEnqueue(T item) {
            if (item == null) {
                throw new NullPointerException("Item cannot be null");
            }

            long currentTail = (long) TAIL_HANDLE.getAcquire(this);
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);

            if (currentTail - currentHead >= capacity) {
                return false;
            }

            int index = (int) (currentTail & mask);
            buffer[index] = item;
            TAIL_HANDLE.setRelease(this, currentTail + 1);
            return true;
        }

        @SuppressWarnings("unchecked")
        public T tryDequeue() {
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);

            if (currentHead == currentTail) {
                return null;
            }

            int index = (int) (currentHead & mask);
            T item = (T) buffer[index];
            buffer[index] = null;
            HEAD_HANDLE.setRelease(this, currentHead + 1);
            return item;
        }

        public int size() {
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);
            return (int) (currentTail - currentHead);
        }
    }
}