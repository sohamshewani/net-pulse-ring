# net-pulse-ring
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/net-pulse-ring/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/net-pulse-ring/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/net-pulse-ring) [![Stars](https://img.shields.io/github/stars/sohamshewani/net-pulse-ring?style=flat)](https://github.com/sohamshewani/net-pulse-ring)

## Executive Summary & Problem Space

`net-pulse-ring` is a high-throughput, low-latency, zero-copy packet processing and flow control engine engineered in Java for high-frequency networking environments. Standard JVM networking paradigms suffer from catastrophic latency degradation under heavy load due to garbage collection (GC) pauses, thread context switching, lock contention on shared queues, and redundant memory copies between kernel and user space.

`net-pulse-ring` resolves these systemic bottlenecks by implementing:
- **Off-Heap Direct Memory Arenas (`MemoryArena.java`)**: Bypasses the JVM garbage collector entirely for payload handling, eliminating stop-the-world pauses.
- **Lock-Free Single-Producer Single-Consumer (SPSC) Rings (`LockFreeSpscRing.java`)**: Leverages sequential consistency, cache-line padding, and acquire-release semantics to achieve bounded, deterministic message passing between IO threads and processing workers without mutexes or spinlock contention.
- **Non-Blocking Polling Loops (`NonBlockingPollLoop.java`)**: Epoll-backed edge-triggered event loops that maximize network I/O concurrency.
- **Hardware-Friendly Packet Filtering & Flow Control (`PacketFilterEngine.java`, `SlidingWindowFlowControl.java`)**: Pre-allocated rule evaluation pipelines combined with dynamic sliding windows to prevent downstream bufferbloat and backpressure failure modes.

---

## Structural ASCII Architecture / Dataflow Diagram

```
 +-----------------------------------------------------------------------+
 |                     OS Kernel / Network Interface                     |
 +-----------------------------------------------------------------------+
                                    | (Raw Socket / Epoll Event)
                                    v
 +-----------------------------------------------------------------------+
 |                    NonBlockingPollLoop (Epoll)                        |
 |  - Edge-Triggered Polling                                             |
 |  - Direct ByteBuffer Allocation via MemoryArena                       |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                    PacketBuffer (Off-Heap Arena)                      |
 |  - Zero-Copy Slices                                                   |
 |  - Reference Counted Off-Heap Buffers                                 |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                  LockFreeSpscRing (Producer Thread)                   |
 |  - Cache-Line Padded Head/Tail Pointers                               |
 |  - AtomicMemory Order Primitives (Volatile / VarHandle)               |
 +-----------------------------------------------------------------------+
                                    |
                                    v (Consumer Thread)
 +-----------------------------------------------------------------------+
 |                    PacketFilterEngine (Pipeline)                      |
 |  - Stateless / Stateful Rule Evaluation                               |
 |  - Bitmask Inspection & Drop/Allow Decision                           |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |               SlidingWindowFlowControl (State Machine)                |
 |  - Congestion Window Management                                       |
 |  - Rate Limiting & Acknowledgement Tracking                           |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                         Application Consumer                          |
 +-----------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
`net-pulse-ring` strictly avoids general-purpose monitors (`synchronized`, `ReentrantLock`) on the hot path. Synchronization is managed through lock-free memory rings (`LockFreeSpscRing.java`) operating on single-producer/single-consumer boundaries. Contention is entirely eliminated by enforcing thread-affinity: exactly one producer thread modifies the write cursor, and exactly one consumer thread modifies the read cursor.

### Memory Ownership & Off-Heap Allocations
All packet payloads reside in native memory allocated via `MemoryArena.java`. 
- **Invariant 1**: Buffers are immutable regarding their allocation boundary once instantiated.
- **Invariant 2**: Reclaim operations are deterministic. When a `PacketBuffer` reference count hits zero, memory blocks are immediately recycled back to the arena free-list without invoking the Garbage Collector.
- **Invariant 3**: Cache-line false sharing is prevented by padding structural head and tail sequence variables to 128 bytes (two cache lines), ensuring core-to-core cache coherence ping-ponging is minimized.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Target Component | Time Complexity (Big-O) | Space Complexity | P99 Latency Estimate |
| :--- | :--- | :--- | :--- | :--- |
| **Ring Enqueue** | `LockFreeSpscRing.java` | $O(1)$ | $O(1)$ (Pre-allocated) | $< 15\text{ns}$ |
| **Ring Dequeue** | `LockFreeSpscRing.java` | $O(1)$ | $O(1)$ (Pre-allocated) | $< 12\text{ns}$ |
| **Packet Filtering**| `PacketFilterEngine.java` | $O(K)$ ($K$ = Rules) | $O(1)$ | $< 45\text{ns}$ |
| **Flow Control Tick**| `SlidingWindowFlowControl.java`| $O(1)$ | $O(W)$ ($W$ = Window Size)| $< 10\text{ns}$ |
| **Arena Allocation**| `MemoryArena.java` | $O(1)$ | $O(N)$ ($N$ = Arena Pool)| $< 20\text{ns}$ |

---

## Security Architecture & Threat Model

### Input Boundaries & Sanitization
All incoming socket bytes are treated as untrusted input. `PacketFilterEngine.java` enforces strict structural bounds-checking before header parsing occurs, dropping malformed packets or truncated frames before memory allocation completes.

### Memory Corruption & Out-of-Bounds Protections
Because pointers are manipulated off-heap via Java’s `VarHandle` and `Unsafe` APIs, boundary enforcement relies on strict arithmetic validations within `PacketBuffer.java`.
- **Buffer Overflows**: Prevented by strict length checks matching the advertised socket payload size against the pre-allocated arena slab size.
- **Use-After-Free**: Mitigated via atomic reference counting. Once a buffer is released to the `MemoryArena`, subsequent attempts to write or read via stale references trigger immediate assertion errors or security traps.

---

## Build, Verification & Benchmarking Recipes

Ensure your environment satisfies the prerequisites: **Java 17+ (JDK)**, **Maven 3.8+**, **Python 3.9+**, and **pytest**.

### 1. Compile and Package the Project
```bash
mvn clean package -DskipTests=false
```

### 2. Run Invariant Verification Tests
Execute the Python-driven invariant test suite to validate concurrency bounds, ring-buffer integrity, and memory safety invariants:
```bash
python3 -m pytest tests/test_invariants.py -v
```

### 3. Execute Engine Benchmarks
Run the high-performance benchmark suite to profile throughput, packet latency distributions, and filter execution rates:
```bash
python3 benchmarks/bench_engine.py --duration=30 --threads=4