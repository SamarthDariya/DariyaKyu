# dariyakyu

A Kafka-style distributed commit log, written from scratch in C++.

*"Dariya kyu?"* — "why, Dariya?", the question people ask about the author — plus **queue**.
Project #2 of the `dariyan_world` series, after
[dariyanache](https://github.com/SamarthDariya/DariyanAche) ("Dariya nāche", Dariya dances — a
Redis clone).

This is a learning project: the goal is to understand low-level and high-level design by
building the real thing, not to compete with Apache Kafka.

---

## What it is

An append-only, segmented, partitioned commit log with a broker in front of it. Producers append
records; consumers read them back by offset at their own pace. Data is stored **once** and read
by as many consumers as want it — reading consumes nothing and deletes nothing.

The design is written up in full, with the reasoning and the rejected alternatives, in
**[DESIGN.md](DESIGN.md)**. Its one-line thesis:

> **The broker is deliberately ignorant.** It does not decompress batches, does not route
> messages, does not track per-message state, does not know what "processing" means, and does
> not compute consumer group assignments. Every one of those refusals is why it is fast.

---

## Status

**Design: complete.** 23 conceptual decisions locked, structural design in progress.
**Implementation: M0.**

| Phase | Status |
|---|---|
| Conceptual design — 23 decisions, `DESIGN.md` Part I | ✅ complete |
| Structural design — classes, ownership, hot paths, `DESIGN.md` Part II | 🔸 3 of 6 chunks |
| Implementation — M0…M9 | 🔸 M0–M1 complete, M2 in progress |

---

## Checkpoints

Each milestone ends in something runnable or testable, lives on its own `feature/*` branch, and
is merged by PR.

### M0 — Skeleton ✅
- [x] CMake build, C++20, warnings as a matter of course
- [x] ASan/UBSan and TSan build options, mutually exclusive
- [x] `Offset` as a strong type, `TopicPartition`, `Epoch`, `FileRange`
- [x] `Error` / `IoError` / `OffsetInvariantViolated` / `CorruptData`
- [x] `FileHandle` — RAII fd, positional I/O, append reports its own position
- [x] `MappedFile` — RAII mmap, read-only and read-write, preallocate, trim on seal
- [x] doctest wired into `ctest` (header-only, no subproject build)
- [x] suite green locally — 32 cases, 96 assertions
- [x] suite green under ASan/UBSan and under TSan

### M1 — Record batch codec ✅
- [x] varint / varlong with zigzag encoding
- [x] CRC32C (Castagnoli), portable table implementation
- [x] `BufferReader` / `BufferWriter`, big-endian, borrowing reads, patchable fields
- [x] Kafka v2 `RecordBatch` encode + decode, byte-exact
- [x] header fields readable without touching the body
- [x] in-place base offset and leader epoch stamping, checksum untouched
- [x] null vs empty distinguished (tombstones survive)
- [x] record headers parsed and preserved
- [x] round-trip and corruption tests — 32 cases, 203 assertions
- [x] green under ASan/UBSan and TSan

### M2 — The log engine 🔸
- [x] `OffsetIndex` — fixed-size entries, binary search, mmap'd
- [ ] `SegmentBase` / `SealedSegment` / `ActiveSegment`
- [ ] sparse indexing every 4 KB of log written
- [ ] segment rolling on size **or** age
- [ ] `Log` — append, read, roll, truncate
- [ ] crash recovery: CRC scan, truncate at first bad batch
- [ ] `Log::read` returns a `FileRange`, never bytes

### M3 — Topics and retention ⬜
- [ ] `LogManager` — partition registry, startup scan
- [ ] `partition.meta` — self-describing partitions
- [ ] retention by age **and** by partition bytes
- [ ] maintenance thread: retention **and** age-based rolling of idle partitions
- [ ] `OffsetOutOfRange` on reads below the log start

### M4 — First real broker ⬜
- [ ] wire framing, request header, `correlationId` pipelining
- [ ] `ApiRegistry` dispatch, `BrokerContext`
- [ ] `Metadata`, `CreateTopic`, `Produce`, `Fetch`, `ListOffsets`
- [ ] per-partition error codes
- [ ] thread-per-connection server
- [ ] `sendfile` on the read path
- [ ] `dariyakyu-cli` — produce, consume, describe, dump-segment
- [ ] **produce and consume from a terminal**

### M5 — Consumer groups ⬜
- [ ] `__offsets` internal topic
- [ ] `GroupCoordinator` — leader of `hash(group) % N`
- [ ] `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`
- [ ] `OffsetCommit` / `OffsetFetch`, commits storing *next* offset
- [ ] range and round-robin assignors, computed client-side
- [ ] generation-id fencing
- [ ] eager rebalance on join, leave, and heartbeat expiry

### M6 — Compaction ⬜
- [ ] `LogCleaner` — key → latest offset map
- [ ] segment rewrite and atomic swap
- [ ] tombstones (null value) and their deletion horizon
- [ ] `cleanup.policy = delete | compact`
- [ ] `__offsets` stays bounded under sustained commits

### M7 — Replication ⬜
- [ ] `ReplicaFetcher` — followers reuse the consumer `Fetch` API
- [ ] ISR tracking by lag time
- [ ] high watermark = min LEO across ISR; consumers read only committed data
- [ ] `acks` 0 / 1 / all, `min.insync.replicas`
- [ ] idempotent producer — producer id + sequence numbers
- [ ] two-node durability demo

### M8 — Controller and failover ⬜
- [ ] broker registration and heartbeats
- [ ] `LeaderAndIsr`, `UpdateMetadata`
- [ ] leader election on broker failure
- [ ] leader epochs stamped on every request, `FENCED_LEADER_EPOCH`
- [ ] `leader-epoch-checkpoint` and epoch-based truncation
- [ ] unclean leader election off
- [ ] **kill the leader, watch it recover**

### M9 — Threading and performance ⬜
- [ ] `kqueue` network threads
- [ ] request/response queues, I/O thread pool
- [ ] `sendfile` moved off the network threads
- [ ] TSan green under load
- [ ] benchmarks: `sendfile` vs copying read path, batch size sweep, fsync cost

---

## Build

```sh
brew install cmake            # one-time

cmake -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

With sanitizers:

```sh
cmake -B build-tsan -DDARIYAKYU_TSAN=ON && cmake --build build-tsan -j
cmake -B build-asan -DDARIYAKYU_ASAN=ON && cmake --build build-asan -j
```

Requires a C++20 compiler (developed against Apple Clang 17). doctest is fetched at configure
time and is the only dependency.

**Platform:** developed on macOS, so `kqueue`, BSD `sendfile`, and `F_NOCACHE` rather than
`epoll`, Linux `sendfile`, and `O_DIRECT`. There is no `io_uring` here. Platform syscalls sit
behind one header so a Linux port stays small.

---

## Deliberately out of scope

Documented in [DESIGN.md](DESIGN.md), not forgotten:

| | Why | Where it goes |
|---|---|---|
| Full Raft / KRaft consensus | a project in itself | dedicated future project |
| Cooperative rebalancing, static membership | intricate two-phase protocol | Tier A |
| Transactions / exactly-once | large subsystem | Tier A |
| Kafka wire compatibility | weeks of serialisation, little learning | Tier A |
| Follower fetching (rack-local reads) | optimisation with its own hazards | Tier A |
| Tiered storage | out of scope entirely | — |

The **record batch format on disk copies Kafka's v2 layout exactly**, because that part *is*
educational. The request protocol is our own, shaped like Kafka's so compatibility can be added
later as a translation shim rather than a redesign.

---

## Roadmap beyond M9

**Tier A** — broad API coverage, the scoped-out items above, hardening, and enough polish to use
as a real test double.
