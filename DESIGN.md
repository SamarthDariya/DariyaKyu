# dariyakyu — Design

A Kafka-style distributed commit log, in C++.

*"Dariya kyu?"* — "why, Dariya?", the question people ask about the author — plus **queue**.
Project #2 of the `dariyan_world` series, after [dariyanache](../dariyanache) ("Dariya nāche",
Dariya dances — a Redis clone).

---

## The thesis

> **The broker is deliberately ignorant.**
> It does not decompress batches. It does not route messages. It does not track per-message
> state. It does not know what "processing" means. It does not compute group assignments.

Every one of those refusals is a deliberate choice, and together they are why the design is
fast and why the broker is simple. Most of the decisions below are consequences of it.

---

## Part I — Conceptual design

23 locked decisions, derived from first principles. Each records *why*, and what was rejected.

### 0. Language and style

**C++**, not Go.

The Redis clone was written in Go, and its code stopped being reviewed partway through because
it was hard to read. On these projects the review *is* the learning, so reading fluency
outweighs everything else. C++ also opens the systems-performance half of the education
(zero-copy, page cache behaviour, cache-aware layout) that a GC'd language hides.

House style — readability is the point, so:

- `using namespace std;` in `.cpp` files only, never in headers (it leaks into every includer)
- `.hpp` / `.cpp` split, CMake, targeted includes (no `bits/stdc++.h` — GCC-only, absent on Clang)
- RAII for anything owning a file descriptor
- no Boost, no template metaprogramming, no C++20 modules, no `auto` where the type isn't obvious
- if a construct makes the reader stop and squint, it doesn't go in

**Platform:** developed on macOS. That means `kqueue` (not `epoll`), BSD `sendfile` (different
signature from Linux's), `F_NOCACHE` instead of `O_DIRECT`, and **no `io_uring`**. All
platform-specific syscalls hide behind one thin header so Linux support is a small file later.

---

### The core model

#### 1. Data is stored once

An append-only log per partition — not a queue per consumer.

The alternative (broker holds a queue per subscriber) means one produced message becomes N
copies, and a consumer offline for an hour forces the broker to retain everything it hasn't
taken, with no bound.

#### 2. Consumers hold an offset

A consumer's entire state is one integer: *"I have processed everything before this."*

Ordering makes this possible. A general work queue must track a *set* of acknowledged
messages; a strictly ordered log that is read front-to-back collapses that set to its
boundary. 10M messages × 200 consumers goes from 2 billion durable flags to 200 integers.

The consequences matter more than the space saving:

- **Reads stop writing.** Serving a consumer mutates no broker state, so the read path is
  pure, concurrent, lock-free — which is why fan-out to hundreds of consumers is nearly free.
- **Replay is free.** "Re-read the last hour" is `offset -= N`. Reading consumes nothing.
- **The broker forgets consumers exist.** They are not in the data path.

#### 3. No per-message acknowledgement

Rejected deliberately, though SQS and Pulsar do it. Tracking "500 acked, 501 not, 502 acked"
needs a gap bitmap, which puts a **durable write on the read path** and grows without bound if
one message is never acked.

The cost is honest: a poison message is the consumer's problem. It commits past the message and
writes it to a dead-letter topic itself. **The broker never learns that a message failed.**

#### 4. Retention is time and size based, never consumption based

Reading deletes nothing. Data leaves when it ages out or the partition exceeds its byte limit —
whichever comes first — and not before.

A consumer that lags past the retention window loses data. That is accepted and stated up
front: retention is a promise about time, not about consumers. The alternative is letting one
dead consumer grow the disk until the broker dies, taking every healthy consumer with it.
Consumer lag is therefore the primary operational metric.

#### 5. Parallelism comes from partitions

A partition is a **physically separate log file**, and the split happens at *write* time.

This is forced by zero-copy. The fast read path is one syscall — "kernel, send bytes
`[position, length)` of this file to this socket" — which requires the bytes a consumer wants to
be **contiguous**. If parallelism were achieved by handing different consumers different
*subsets* of one file (say, every Nth record), the broker would have to either filter records
in user space or ship the whole log to everyone. Both destroy `sendfile`.

So parallelism must be a property of the physical layout, not of the delivery logic.

Note what this does *not* require: processing N messages concurrently on one machine needs no
partitions at all — that's batching plus threads. Partitions exist for **separate machines
making independent progress**.

#### 6. `partition = hash(key) % partition_count`

Ordering is guaranteed **within a partition and nowhere else** — N independently-appended files
share no clock and no position.

So the partition must be chosen by whatever identifies the thing whose order matters. All
events for `account-42` land in one file, are read by one consumer, and stay ordered. Events
for different accounts have no ordering relationship, and nobody needed one.

| Requirement | Configuration |
|---|---|
| Total order | 1 partition (and therefore no parallelism) |
| Per-key order | N partitions, keyed |
| No order | N partitions, round-robin |

**Order and parallelism are the same dial.**

#### 7. Consumer groups

An offset belongs to `(group, topic, partition)` — not to a consumer process.

A partition is assigned to **at most one member** of a group; two members sharing a partition
would have to coordinate one integer, which is per-message ack tracking again. So partition
count is the ceiling on a group's parallelism, and surplus members idle.

Groups are independent: a slow group and a fast group read the same bytes at their own speeds
and never interact.

#### 8. Partition count is fixed at topic creation

Changing it changes `hash(key) % N`, so a key's history splits across two files with no
ordering relationship between them — key ordering breaks permanently for existing keys.

Known better answer, deliberately deferred: dariyanache hashed to 16384 **fixed slots** and
moved slots between nodes, so the key→slot mapping never changed. Kafka has no such
indirection — partition count *is* the modulus. Revisit in Tier A.

Requires durable **topic metadata** (name, partition count, retention config) as broker state.

#### 9. Fetches are multi-partition

One request carries a list of `(partition, offset, max_bytes)`; one response carries a batch per
partition. Otherwise a member owning 10 partitions makes 10 round trips per poll.

---

### Storage

#### 10. The read path is a sparse index

Finding offset 4,500,000 in an 8 GB file of variable-length records: scanning from byte 0 is
O(n) per reconnect; a full index is 36 MB per partition and 36 GB across a thousand of them.

So: index **every 4 KB of log written**. Note the unit — bounding *messages* scanned leaves the
bytes unbounded (10 messages could be 500 KB); bounding *bytes* directly caps every lookup at a
4 KB forward scan regardless of message size.

Entries are fixed-size `(relative offset, physical position)`, so the index is an array on disk
and lookup is binary search by arithmetic — **no tree**. That works because an append-only
immutable log produces an index that is append-only and *already sorted*. A B-tree exists to
keep sorted data sorted under random insertion; there is no random insertion here.

The index is `mmap`ed, so the OS decides what stays resident. Hot partitions keep their index
cached, cold ones get evicted, and we write no cache.

#### 11. A partition is a directory of segments

Files rolled by size **or** age, each named by the base offset of its first record, each with
its own index whose positions are relative to that segment's start.

Retention then deletes whole files: `unlink()` the oldest segments. O(1), disk freed
immediately, no surviving byte moves, and no surviving index entry changes — because a
segment-relative index never referenced anything outside its own file.

POSIX makes this safe for readers mid-stream: unlinking a file that a reader has open removes
the directory entry but keeps the inode alive until the last descriptor closes. No locks, no
coordination, no torn reads.

**Rejected: reusing space within one file** (a circular buffer). Fixed capacity can't express
"7 days"; wrapping breaks the contiguity `sendfile` requires; and overwriting bytes a slow
consumer is reading either corrupts its read or forces per-consumer tracking back onto the
write path.

What segments buy beyond deletion:

- **Only the active segment is ever written**, so every older segment is immutable and readable
  with no synchronisation at all
- **Crash recovery is bounded** — validate the active segment, not the whole partition
- **Compaction is tractable** — rewrite one segment and swap it, rather than an 8 GB rewrite
- **Segment lookup is a binary search over filenames**

Segment size is the granularity of every whole-file operation. Too small: many files and
descriptors, frequent rolls, and fetches that span segments can't be served by one `sendfile`.
Too large: retention overshoots by up to one segment, crash recovery scans more, compaction
needs more headroom — and on a low-traffic topic the active segment may never roll, so
retention never fires at all. Hence rolling on **age as well as size**. Default 1 GB / 7 days,
tunable per topic.

#### 12. Retention

Delete a sealed segment when its newest record is older than `retention.ms`, **or** when the
partition exceeds `retention.bytes` (which is *per partition*, a classic operational
foot-gun). Enforced by a periodic background sweep, so the limit is approximate by design. The
active segment is never deleted.

Reading a deleted offset returns an error, and the client's reset policy (earliest / latest /
fail) decides what happens.

#### 13. The batch is the unit of storage and transfer

Not the record. A batch is one header — base offset (absolute), length, CRC32C, timestamp base,
attributes — plus N records carrying **varint deltas** for offset and timestamp.

Batching earns its place three times over:

- **Framing overhead** — a ~25-byte header on a 50-byte message is 33% of your disk and
  network; amortised over 500 records it's negligible
- **Syscalls** — one `write`, one round trip, instead of 500
- **Compression** — a single 50-byte JSON message can't be compressed meaningfully; 500
  similar ones share nearly all their structure and compress 5–10×

And the property that makes it beautiful: **the broker never decompresses.** The producer
compresses, the broker writes those exact bytes, and `sendfile` ships those exact bytes to the
consumer, which decompresses. Compression cost lands once, at the edge, never on the broker,
and zero-copy survives end to end.

The delta encoding exists to protect that: the broker stamps the base offset into the header on
arrival and writes the body through untouched. Absolute offsets per record would force it to
decompress, rewrite, and recompress every batch.

Producers flush a batch on **size or time** (`batch.size` / `linger.ms`) — size alone starves a
low-traffic topic, time alone wastes throughput on a busy one. (Third appearance of this
pattern, after the index interval and segment rolling: **always bound by size and time
together.**)

Costs: latency bounded by `linger.ms`, and reads become batch-granular — asking for offset
4,500,003 when the batch starts at 4,500,000 returns the whole batch and the consumer discards
the first three.

**Record layout.** Fields, and why each exists:

| Field | Why |
|---|---|
| length prefix | without it a variable-length stream is unwalkable — the index lands you at a byte and says "scan forward" |
| CRC32C | a crash mid-write leaves a partial record that looks like plausible data; recovery truncates the active segment at the first bad CRC |
| attributes | compression codec and flags |
| timestamp | time-based retention, and seeking by wall clock |
| key length + key | the key is variable-length, and compaction needs it |
| value length + value | the payload |

Deliberately **absent**: the partition (it's the file path) and any ordering key (it's the
physical position). Storing what the layout already encodes is the mistake to avoid.

The on-disk format copies **Kafka's v2 RecordBatch** exactly — see decision 19.

---

### Durability and replication

#### 14. No `fsync` on the write path

When `write()` returns, the data is in the kernel's **page cache** — RAM. It survives the
process being killed, because the kernel didn't die. It does **not** survive a power cut.

`fsync` is the only fix, and it costs 50–200 µs on good NVMe, ~1 ms on cheap SSD, ~10 ms on a
platter. Calling it per batch caps the broker at a few thousand writes/sec no matter how good
the code is.

So durability comes from **replication instead**: the batch is in the page cache of three
machines before the producer is told OK. Losing it requires simultaneous power loss across all
three, so they go in different racks. **Three copies in RAM beat one copy on a platter** — and
a network round trip to two peers is cheaper than one `fsync`.

`flush.messages` / `flush.ms` exist as knobs; the default is to never force.

Mapping back to dariyanache: **the log *is* the AOF.** There is no separate in-memory structure
to rebuild — the segment files are the data. And **log compaction is the RDB analogue**: a
regular topic is the full history, a compacted topic is the current value of every key.

#### 15. Leaders, followers, ISR

Each partition has a leader and followers. The leader tracks the **in-sync replica set** — those
caught up within `replica.lag.time.max.ms`. A follower that falls behind is ejected, so one sick
machine can't freeze writes.

`acks` is the producer's durability choice:

| `acks` | Meaning | Failure mode |
|---|---|---|
| `0` | fire and forget | loses everything, silently |
| `1` | leader has it | leader dies before replication → **silent** loss; producer was told success |
| `all` | all ISR have it | slowest, and see the trap below |

**The trap:** `acks=all` waits for the *in-sync* replicas. If followers fall behind and get
ejected, ISR shrinks to `{leader}` and `acks=all` has silently become `acks=1` — with no error
anywhere. **`min.insync.replicas`** is the fix: reject the write with `NOT_ENOUGH_REPLICAS`
rather than accept it under false pretenses.

Real durability is all three together:

```
replication.factor  = 3     three copies exist
min.insync.replicas = 2     refuse writes unless two have it
acks                = all   wait for those two
```

That is an explicit CAP choice: losing two brokers makes the partition **unwritable** (still
readable). Errors are preferred over data we might lose.

**Unclean leader election is off.** If every ISR member is dead, the partition stays offline
rather than promoting a stale replica and silently discarding everything it missed.

#### 16. High watermark

`HW = min(log end offset across ISR)`. Consumers may read **only** up to it.

The justification isn't tidiness — it's that a consumer *acts* on what it reads. Read offset 95,
charge a card, send the email; then the leader dies and a follower with only 90 takes over, and
offset 95 never happened. You cannot un-charge the card. **The log must never appear to move
backwards**, because downstream side effects are irreversible.

Mechanics: followers learn the HW one round trip late (piggybacked on fetch responses), which
only ever makes them more conservative. On failover the new leader's log is the truth, and
followers **truncate** anything above it — legitimate precisely because those records were never
committed and never acknowledged.

Consequence worth knowing: **replication latency is on the read path too.** Even with `acks=0`,
nothing is visible until the HW advances. One slow follower delays every consumer of that
partition.

Reads are served by the leader only.

#### 17. Cluster management: a designated controller

Chosen from three options:

| | | |
|---|---|---|
| A | fully static assignment, manual failover | too little |
| **B** | **static assignment + one designated (not elected) controller broker** | **chosen** |
| C | full Raft/KRaft consensus | a project in itself |

The controller watches heartbeats and reassigns leadership when a broker dies, giving automatic
failover — where the interesting reasoning lives (heartbeats, split brain, fencing) — without
writing consensus. The controller is a **single point of failure**, documented as such. Raft is
banked for a dedicated future project, exactly as it was at dariyanache's M7c.

#### 18. Leader epochs (fencing)

The problem B forces: the controller declares broker 1 dead after a 6-second GC pause and
promotes broker 2. Broker 1 wakes up healthy, still believing it leads partition 3, and some
producers still have it cached. Two leaders, both accepting writes at the same offsets.

**No timeout value fixes this** — you can never distinguish "dead" from "slow" over a network.
So the fix isn't detection; it's making the stale leader's writes *harmless*.

Every leadership change increments a monotonic **leader epoch**. Every request carries the
sender's epoch; every receiver remembers the highest it has seen.

- incoming `<` known → reject (`FENCED_LEADER_EPOCH`)
- incoming `>` known → accept and adopt (this is how leadership changes propagate)

The fenced leader learns from the rejection, steps down, and becomes a follower. Producers get
`NOT_LEADER_FOR_PARTITION`, refresh metadata, and find the new leader.

> **Timing affects availability, never correctness.** No amount of network weirdness makes 7
> greater than 8.

This is the general **fencing token** pattern — Raft's *term*, ZooKeeper's *zxid*, Paxos's
*ballot*. It appears again in this design as the consumer group's generation id.

The epoch is also **written into the log**, so a returning follower asks "what was your last
offset in epoch 7?" and truncates exactly there, rather than blindly truncating to the high
watermark. Kafka shipped without this and had a real log-divergence bug.

---

### Protocol and execution

#### 19. Our own wire protocol, Kafka's record format

**Requests are ours** — length-prefixed, Kafka-shaped header (`api_key`, `api_version`,
`correlation_id`), where the correlation id allows multiple in-flight requests on one
connection.

Full Kafka wire compatibility was rejected: it's hundreds of pages of versioned schemas with
flexible encoding, and a minimal useful subset is weeks of byte-exact serialisation *before
storing a single record*. That teaches serialisation, not log design. (The `redis-cli` analogy
from dariyanache breaks here — RESP is a one-page spec.)

**But the on-disk record batch copies Kafka's v2 format exactly.** That part *is* educational —
varints, CRC placement, delta encoding, compression framing — and it's self-contained.

Compensating for the lost oracle: a `dariyakyu-cli` of our own, and **real Kafka in Docker** to
compare *behaviour* against (same operations, same expected outcomes). Byte compatibility
catches typos; behavioural comparison catches design mistakes.

Wire compatibility is a Tier-A stretch goal — the framing keeps Kafka's shape, so it becomes a
translation shim rather than a redesign.

**Discovery:** every broker knows the full cluster map, so any broker can answer a `Metadata`
request and a client only needs one reachable bootstrap address. The client computes
`hash(key) % N` **itself** and connects straight to the leader — the broker never routes,
forwards, or proxies. Stale metadata is normal and self-heals: send to the old leader, get
`NOT_LEADER_FOR_PARTITION`, refresh, retry. Retry logic and metadata refresh are the same loop.

(dariyanache redirected with `MOVED`; Kafka pre-fetches the map. Redis's way costs a round trip
when wrong, Kafka's needs a richer metadata API. Neither proxies.)

**Followers reuse the consumer `Fetch` API** — a follower is just a consumer that writes what it
reads into its own log. Same request, same code path, same zero-copy read on the leader.

| Purpose | Requests |
|---|---|
| Discovery | `Metadata`, `CreateTopic` |
| Data | `Produce`, `Fetch`, `ListOffsets` |
| Groups | `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetCommit`, `OffsetFetch` |
| Replication | `Fetch` (reused) |
| Control plane | `LeaderAndIsr`, `UpdateMetadata`, broker `Heartbeat` |

#### 20. Threading: network threads plus an I/O pool

The single-threaded event loop from dariyanache M7b is **wrong here**, and the reason is
instructive. There, every operation was a RAM hash lookup — sub-microsecond, never blocking — so
serialising everything cost nothing and bought total safety. Here a fetch that misses the page
cache blocks for milliseconds, and on a single-threaded loop *every other client waits for that
one disk seek*.

```
sockets ──▶ [ kqueue network threads ] ──▶ request queue ──▶ [ I/O thread pool ] ──▶ disk
              never touch the disk                              blocks freely
                     ▲                                                │
                     └──────────────── response queue ◀───────────────┘
```

Plus dedicated background threads: replica fetchers, the retention sweeper, the log cleaner.

**Threads, but barely any locks** — because immutability already removed the sharing:

- old segments are immutable → any number of concurrent readers, no synchronisation
- the active segment has exactly one appender → single writer publishes the high watermark with
  an atomic store; readers only read below it
- what does need a lock is rare: rolling a segment, deleting segments, metadata changes

**Trap:** `sendfile` blocks — it *is* a disk read. It belongs on an I/O thread. Running it on a
network thread makes the whole architecture pointless, and it's the easiest mistake to make.

**Rule: exceptions never cross a thread boundary.** An exception that escapes a thread's
top-level function calls `std::terminate` and kills the entire broker. So every worker thread
wraps its unit of work:

```cpp
try                            { handle(request); }
catch (const Error& e)         { respondWithError(mapToWireCode(e)); }
catch (const std::exception& e){ log(e.what()); respondWithError(ErrorCode::Unknown); }
```

Without it, one `IoError` from a full disk takes down a broker that could have returned an error
code for the affected partition and kept serving every other one. The blast radius of a failure
should be the request, not the process.

Early milestones use **thread-per-connection** (blocking, no event loop) so the log engine can
be built without an event loop in the way; a dedicated milestone converts to the model above.
This mirrors dariyanache's path, with the lesson inverted: there we moved to *fewer* threads
because everything was fast, here we move to a *pipeline* because some things are slow.

---

### Consumer groups and delivery semantics

#### 21. Offsets live in a compacted internal topic

Offsets need to be durable, replicated, write-heavy and read-rarely — which describes a system
we already have. So `__offsets` is an ordinary compacted topic:

```
key: (group, topic, partition)  →  value: (offset, metadata, timestamp)
```

**Compaction collapses AOF and RDB into one mechanism** — an append-only log that self-truncates
into a snapshot. dariyanache needed both, plus an awkward crash window between renaming the
snapshot and truncating the AOF. One mechanism, no coordination, no crash window.

**The coordinator for a group is the leader of `hash(group_id) % N` of that topic.** No new
election, no new failover path — leadership, replication and fencing are the mechanisms already
built for data partitions. Clients locate it with `FindCoordinator`.

The coordinator serves from an in-memory map, rebuilt on failover by replaying the partition —
which is dariyanache's AOF replay, down to the shape of the code.

**A commit stores the next offset to read**, not the last one processed. Handle 500–509, commit
510. Getting this wrong reprocesses or skips exactly one message per restart, survives testing,
and shows up in production.

Placement of the commit is the consumer's semantics choice:

- commit → process → crash between = **at-most-once** (lost)
- process → commit → crash between = **at-least-once** (reprocessed) ← default

#### 22. Rebalancing: the client computes the assignment

The broker knows everything and is the obvious choice, so the fact that Kafka does the opposite
is the interesting part.

Assignment strategies are application-specific and unpredictable: range, round-robin, sticky,
rack-aware, capacity-weighted, co-partitioned for joins, statically pinned. Broker-side logic
would mean **a cluster upgrade and rolling restart for every new strategy**, coordinated with
every team using it. Client-side, a strategy is a class in one app and rolling it out is one
deploy.

It works because the broker doesn't need to *understand* the assignment — it relays opaque
bytes. (Fifth appearance of deliberate ignorance.)

```
1. members ──JoinGroup(subscription, strategies)──▶ coordinator
2. coordinator picks the first member as group leader, bumps generation → 5
3. ◀── JoinGroup response: leader receives the full member list, others receive nothing
4. leader computes the assignment locally
5. leader ──SyncGroup(opaque assignment)──▶ coordinator
6. ◀── SyncGroup response: each member receives only its own slice
7. members fetch; heartbeats maintain liveness
```

**The generation id is a fencing token** — third appearance of the pattern. A zombie member from
generation 4 that wakes up and commits gets rejected because the group is on 5.

Cost: rebalancing is **stop-the-world**. Every member revokes everything and processing halts
until the new assignment lands, so one restarting consumer freezes the group for seconds.

Scope: **eager rebalancing only**, with range and round-robin strategies. Cooperative
(incremental) rebalancing and static membership are Tier A — the pain that motivates them
should be felt first.

#### 23. Idempotent producer, no transactions

At-least-once produces duplicates from two sources: a consumer crashing before it commits, and a
**producer retrying** after a timeout it never saw the ack for.

The producer half is cheap to fix: a producer id plus a per-partition sequence number, and the
broker rejects a duplicate sequence. **In scope.**

Full exactly-once — transactions spanning partitions, a transaction coordinator, two-phase
commit, consumers filtering aborted records — is a large subsystem. **Tier A.**

---

## Scoped out (documented, not forgotten)

| | Why | Where it goes |
|---|---|---|
| Full Raft / KRaft consensus | a project in itself | dedicated future project |
| Cooperative rebalancing, static membership | intricate two-phase protocol | Tier A |
| Transactions / exactly-once | large subsystem | Tier A |
| Kafka wire compatibility | weeks of serialisation, little learning | Tier A (framing already shaped for it) |
| Follower fetching (rack-local reads) | optimisation with its own hazards | Tier A |
| Tiered storage (offload to object store) | out of scope entirely | — |
| Fixed-slot partitioning (dariyanache's 16384) | fixes repartitioning, but diverges from Kafka | Tier A, decide then |

---

## Part II — Structural design

Built bottom-up, because the log is the star. Every class below exists to serve a decision from
Part I, and the interesting ones encode that decision in the type system rather than in a
comment.

### Intended file tree

Files are created **as each milestone needs them** — the repo never contains hollow
placeholders. This is the shape it grows into.

```
dariyakyu/
├── CMakeLists.txt
├── DESIGN.md
├── src/
│   ├── common/        types, FileHandle, MappedFile, crc32c, varint, platform syscalls
│   ├── storage/       RecordBatch, OffsetIndex, Segment, Log, LogManager, LogCleaner
│   ├── protocol/      request & response types, codec, framing
│   ├── server/        acceptor, connection, request queue, I/O pool, Broker
│   ├── replication/   ReplicaFetcher, IsrTracker, LeaderEpochHistory
│   ├── cluster/       Controller, ClusterMetadata, TopicConfig
│   ├── group/         GroupCoordinator, Group, Member, OffsetStore, assignors
│   └── cli/           dariyakyu-cli
├── apps/              broker main, cli main
└── tests/
```

### 1. Storage layer

#### Ownership

Strict ownership downward, `unique_ptr`, no back-pointers.

```
LogManager                    every partition this broker hosts
 └── Log                      one partition   (TopicPartition → Log)
      ├── SealedSegment       ordered by base offset, immutable
      ├── ActiveSegment       exactly one, the only writable thing in the partition
      │    ├── FileHandle     .log
      │    └── OffsetIndex    .index   (mmap'd)
      └── LeaderEpochHistory  .epochs
```

`Log` keeps sealed segments in a `std::map<Offset, unique_ptr<SealedSegment>>`. Ordered, so
locating the segment for an offset is `upper_bound` then step back — which is decision 11's
"binary search over filenames," in three lines of standard library.

#### `FileRange` — the type that enforces zero-copy

```cpp
// A location in a file. Never the bytes themselves.
struct FileRange {
    int    fd;
    off_t  position;
    size_t length;
};
```

**Reads return a `FileRange`, not a buffer.** `Segment` and `Log` report *where* the bytes are;
the network layer hands that straight to `sendfile`. If any read path in this codebase ever
returns a `vector<uint8_t>`, zero-copy is dead — so decision 13 is enforced by the return type
rather than by a comment asking politely.

**Amended at M2 — `Log::read` does perform bounded I/O.** This section originally claimed it
performed none. That cannot be true while the index is sparse: an entry every 4 KB gets a lookup
*close* to its target, and the batch boundary can only be found by reading batch headers forward
from there. Kafka does the same. What survives, and is the part decision 13 actually needs, is
that the read path reads **headers only, never a record body** — so the payload still leaves by
`sendfile`, untouched, and the scan is bounded by one index interval (a page or two, normally
already page-cached). The "lock held for nanoseconds" line goes with it: the shared lock is held
across that scan.

#### A range starts on a batch boundary; it does not end on one

**Added at M2.** The range's start is exactly the first byte of the batch *containing* the
requested offset — a batch is the unit of transfer, so a consumer asking for offset 503 of a
batch spanning 500..504 receives the whole batch and skips what it already has.

The end is simply capped at the fetch size, wherever that falls. Aligning it would mean parsing a
header per batch in the range — one `pread` each, so roughly a thousand syscalls for a 1 MB fetch
of small batches — to save the consumer a check it has to make anyway. So the fetch contract is
Kafka's: **a response may end mid-batch, and the consumer discards an incomplete trailing
batch** rather than treating it as corruption.

One exception, and it is not optional: a range always carries **at least one whole batch** when
one is available, even if that exceeds the fetch size. Without it, a consumer whose fetch size is
smaller than the next batch polls forever and never advances — a permanent stall caused by
nothing but a conservative setting.

#### Reads return a result, not an exception

A read has three outcomes, and they are one integer apart from each other:

| Request | Meaning | Outcome |
|---|---|---|
| `offset < logStartOffset` | aged out by retention | error → client resets to earliest |
| `offset == logEndOffset` | **caught up, nothing new yet** | **success, zero bytes** |
| `offset > logEndOffset` | asking for the future | error → client is confused, or the log was truncated |

The middle row is the most common read in the entire system — every caught-up consumer polls
for an offset that does not exist yet, several times a second, forever. It is not an error and
must not cost an exception. The two error rows must also stay distinguishable, because the
client's reset policy treats them differently.

```cpp
enum class ReadError { None, BelowLogStart, AboveLogEnd };

struct ReadResult {
    ReadError error = ReadError::None;
    FileRange range{};          // zero length when caught up — a valid success
};
```

`ReadError` is a *storage* concept; the request handler maps it to the protocol's wire error
code. Storage never learns what a wire error code is.

Exceptions remain for what is genuinely exceptional: `IoError` (a syscall failed),
`CorruptData` (bad CRC or unparseable header), and `OffsetInvariantViolated` (a segment asked
for an offset it claims to hold but does not).

#### Two segment types, not one flag

Immutability of sealed segments is load-bearing — it's what makes concurrent reads lock-free
(decision 20) and what makes retention safe (decision 11). So the compiler enforces it.

```cpp
// Shared read machinery. Not polymorphic — inherited purely for reuse.
class SegmentBase {
public:
    Offset  baseOffset() const;
    Offset  nextOffset() const;
    size_t  sizeBytes() const;
    int64_t largestTimestamp() const;
    FileRange read(Offset offset, size_t maxBytes) const;
protected:
    Offset      baseOffset_;
    Offset      nextOffset_;
    FileHandle  log_;
    OffsetIndex index_;
    int64_t     largestTimestamp_ = 0;
};

// Immutable. There is no append() to call, on any code path, ever.
class SealedSegment final : public SegmentBase {
public:
    static unique_ptr<SealedSegment> open(const fs::path& logFile);
    void unlinkFiles();                       // retention
};

// The only writable segment in the partition.
class ActiveSegment final : public SegmentBase {
public:
    static unique_ptr<ActiveSegment> create(const fs::path& dir, Offset baseOffset);
    static unique_ptr<ActiveSegment> recover(const fs::path& logFile);   // CRC scan + truncate

    // Single-appender only: this partition's I/O thread and nobody else.
    void append(Offset baseOffset, span<const uint8_t> batchBytes);
    void flush();
    bool shouldRoll(int64_t nowMs) const;              // size OR age OR a full index

    // Consumes the active segment and yields an immutable one.
    static unique_ptr<SealedSegment> seal(unique_ptr<ActiveSegment>);
};
```

**Amended at M2 — three changes to `shouldRoll`.** It takes only the current time: the segment
holds the policy it was created with, and passing a second copy per call let `append` and
`shouldRoll` disagree about the index interval. Time is a parameter rather than a clock read so
tests can advance it without sleeping, and so M3's maintenance thread can read the clock once and
sweep every partition against the same instant. And it has a **third** trigger the original two
did not name: **a full index forces a roll.** Once entries start being dropped, everything
claiming to be bounded by one index interval — the read scan, and the header walk that reopens a
sealed segment — quietly degrades into a scan of the whole segment.

The signature carrying the guarantee is `seal()`: it **consumes** its argument, so after the
call the caller's pointer is null. There is no way to hold a writable handle to a sealed
segment, because the writable object no longer exists. Immutability by ownership, not by
discipline.

`seal()` does real work beyond the type change — final index flush, then **trim the index
file**. Index files are preallocated so appends never extend a live mapping mid-write, which is
also why `OffsetIndex` tracks its entry count separately from the file size.

No virtual functions: `Log` always knows which kind it holds, so the read path dispatches on an
offset comparison rather than a vtable.

```cpp
class OffsetIndex {
public:
    struct Entry { uint32_t relativeOffset; uint32_t position; };   // 8 bytes, fixed
    void append(Offset offset, uint32_t position);
    Entry lookup(Offset target) const;             // greatest entry <= target
private:
    Offset     baseOffset_;
    MappedFile map_;
    size_t     entryCount_;
};
```

**Amended at M2 — no base entry is written, and `lookup` is total a different way.** The plan
here was to write an entry for the segment's own base offset at creation, so that the "greatest
entry ≤ target" search could never come up empty and `lookup` could return a plain `Entry`.

`lookup` does return a plain `Entry`, but by **falling back to `{0, 0}`** — the segment's own
beginning, which is always a correct place to start scanning from. That is strictly more robust,
because it also covers two cases a written base entry cannot: a 0-byte index file, and an index
emptied by recovery. It also keeps the paragraph below true, which a base entry would have
falsified: with one always present, the smallest possible trimmed index is 8 bytes, and the
low-traffic case described there could never arise.

**Position 0 is reserved as a consequence, and it earns its keep.** An index whose segment was
still active when the broker died is preallocated to full length with a zero-filled tail, so
trusting the file size would hand out `{0, 0}` padding as though it were data. `position == 0` is
therefore the marker that separates real entries from padding, found by one binary search rather
than by scanning — which would fault in every page of a 10 MiB index, per segment, at startup.
`append` rejects position 0 to keep that sound, and nothing is lost: `lookup` already answers
"the start of this segment" when it has nothing better, so an entry pointing at byte 0 would
carry no information anyway.

Independently, a **zero-length index file must still open**. A segment that rolled on age with
less than one index interval of data written has no entries at all, and its trimmed index is 0
bytes; refusing to map that would fail to open a valid partition on any low-traffic topic after
a restart. `MappedFile::openReadOnly` therefore treats an empty file as an empty mapping rather
than an error.

#### `Log` — one partition

```cpp
class Log {
public:
    Offset     append(span<uint8_t> batchBytes);        // assigns offsets, may roll
    ReadResult read(Offset offset, size_t maxBytes) const;

    Offset logEndOffset() const;
    Offset highWatermark() const;
    void   setHighWatermark(Offset);

    void truncateTo(Offset);                            // failover
    void applyRetention(const RetentionPolicy&);        // unlink sealed segments
private:
    TopicPartition                          tp_;
    map<Offset, unique_ptr<SealedSegment>>  sealed_;
    unique_ptr<ActiveSegment>               active_;
    atomic<Offset>                          logEndOffset_;
    atomic<Offset>                          highWatermark_;
    LeaderEpochHistory                      epochs_;
    mutable shared_mutex                    segmentsMutex_;
};
```

Two details are direct consequences of Part I:

- **`highWatermark_` is atomic, not mutex-guarded.** One appender publishes it, many readers
  consume it — decision 20's "threads, but barely any locks."
- **`segmentsMutex_` guards the map, never file contents.** Sealed segments are immutable, so
  reading their bytes needs no synchronisation at all. The lock is held only while rolling or
  deleting.

```cpp
ReadResult Log::read(Offset offset, size_t maxBytes) const {
    shared_lock lock(segmentsMutex_);

    if (offset > logEndOffset_.load(memory_order_acquire))
        return {ReadError::AboveLogEnd, {}};
    if (offset == logEndOffset_.load(memory_order_acquire))
        return {ReadError::None, {}};        // caught up: success, zero bytes

    if (offset >= active_->baseOffset())
        return {ReadError::None, active_->read(offset, maxBytes)};

    auto it = sealed_.upper_bound(offset);   // first base > offset
    if (it == sealed_.begin())
        return {ReadError::BelowLogStart, {}};
    --it;                                    // greatest base <= offset
    return {ReadError::None, it->second->read(offset, maxBytes)};
}
```

#### Recovery

On startup `Log` scans its directory:

- every `.log` file except the highest base offset → `SealedSegment::open` (trust it, it was
  sealed)
- the highest → `ActiveSegment::recover`, which walks batches validating CRCs and truncates at
  the first failure

Bounded work regardless of partition size — decision 11's crash-recovery freebie, now visible
in the type signatures.

### 2. `LogManager`, topics, retention, and the write path

#### On-disk layout

```
data/
├── orders-0/
│   ├── 00000000000000000000.log
│   ├── 00000000000000000000.index
│   ├── 00000000000001073741.log
│   ├── 00000000000001073741.index
│   ├── leader-epoch-checkpoint
│   └── partition.meta          ← topic config: the partition is self-describing
├── orders-1/
├── __offsets-12/
└── meta/
    └── cluster.meta            ← controller's durable metadata (temp + rename)
```

`partition.meta` earns its place: `LogManager` recovers every partition's retention and roll
policy from disk alone. A broker that boots while the controller is down still opens its logs
correctly and serves reads.

```cpp
class LogManager {
public:
    LogManager(fs::path dataDir, LogConfig defaults);

    void loadAll();                                       // startup scan
    Log* get(const TopicPartition&) const;                // nullptr if not hosted here
    Log& createPartition(const TopicPartition&, const LogConfig&);
    void removePartition(const TopicPartition&, int64_t nowMs);

    void runMaintenance(int64_t nowMs);                   // one sweep
    void startMaintenance(int64_t intervalMs);            // and the thread that drives it
    void stopMaintenance();
private:
    fs::path                                        dataDir_;
    unordered_map<TopicPartition, unique_ptr<Log>>  logs_;
    vector<RemovedPartition>                        removed_;     // see deferred deletion
    mutable shared_mutex                            logsMutex_;   // topic create/delete only
};
```

**Amended at M3 — three signature changes and one new member.**

`TopicConfig` became `LogConfig`: the thing a partition needs is its roll policy plus its
retention policy, and nothing else. Topic-level configuration that is *not* a log concern —
partition count, replication factor — belongs to the controller at M8 and would have no business
in `partition.meta`.

**Time is a parameter, everywhere.** `removePartition` and `runMaintenance` both take `nowMs`
rather than reading the clock. Tests can then advance time without sleeping, and the sweep reads
the clock once and measures every partition on the broker against the same instant instead of
against slightly different ones.

There is a wart in that, and it is worth writing down rather than discovering twice: `nowMs` is
compared against `firstAppendMs_`, which `Log::append` captures from the wall clock *internally*.
The two are implicitly required to be on the same clock, and nothing enforces it — pass synthetic
times and age-based rolling silently never fires. Fixing it properly means threading a clock
through `append` as well.

`get()` returns a raw `Log*` deliberately — a **non-owning observer**. `LogManager` owns every
`Log` for the broker's lifetime and callers borrow; a `shared_ptr` would imply a lifetime
question that doesn't exist.

#### Deferred deletion: why a `FileRange` outlives its segment

**Added at M3.** This is the decision the ownership diagram above does not survive contact with.

`Log::read` returns a `FileRange` — a raw descriptor and a byte range — which the caller may not
use until later, on an I/O thread. Retention runs on a *third* thread. Destroying a
`SealedSegment` closes its descriptor, so retention deleting a segment while a range is in flight
would fail the send, or worse: the descriptor number gets reused by another file and a consumer is
sent someone else's bytes. Silent, and attributable to anything but retention.

Unlinking is not the problem — POSIX keeps an inode alive while any descriptor is open. So the
fix delays **destruction**, not unlinking:

```cpp
struct BuriedSegment { unique_ptr<SealedSegment> segment; int64_t unlinkedAtMs; };
vector<BuriedSegment> graveyard_;               // in Log

struct RemovedPartition { unique_ptr<Log> log; fs::path directory; int64_t removedAtMs; };
vector<RemovedPartition> removed_;              // in LogManager
```

The directory entry goes immediately, so a restart never finds it; the object lingers for
`segmentDeleteDelayMs` and then goes, which is when the disk space actually comes back. Two
graveyards, because the same hazard exists at two scales: segments retention deleted, and whole
partitions an administrator deleted.

The alternative — handing out `shared_ptr<SealedSegment>` from every read — was rejected. It
contradicts the ownership rule at the top of this chunk and puts an atomic refcount on the hot
read path to solve something that happens once per segment.

Two consequences worth stating, because both look like bugs from the outside:

- **Disk space comes back at sweep time, not at delete time.** With a one-minute delay, a
  partition holds up to a minute of already-deleted segments.
- **The graveyard is rarely empty on a busy partition.** Each sweep buries what has just aged out
  while freeing what was buried a delay ago.

`removePartition` gets one extra step for the same reason plus a second: the directory is
**renamed** to `<name>.<timestamp>.delete` before anything is dropped. One atomic step takes the
partition out of the startup scan's sight, so an interrupted deletion cannot leave a directory
that still looks like a partition but has a hole in its offset sequence — which `Log::open`
refuses, turning a topic deletion into a broker that will not start. The timestamp keeps a
partition deleted, recreated and deleted again from colliding with its own earlier corpse. The
startup scan skips those names and finishes the job, since a restart means there are no in-flight
reads left to protect.

#### `partition.meta`, concretely

**Added at M3.** The layout above says this file exists; the format is binary, big-endian, and
versioned:

```
magic uint32 "DKPM" | version int16 | topic int16-length + bytes | partition int32
maxSegmentBytes int64 | maxSegmentAgeMs int64 | indexIntervalBytes int64 | maxIndexBytes int64
retentionMs int64 | retentionBytes int64 (-1 unlimited) | segmentDeleteDelayMs int64
```

Three decisions in that:

**No checksum.** Every other file here is checksummed; this one is replaced atomically instead —
write a temporary, `fsync` it, `rename` over the target, `fsync` the directory. A torn mixture of
old and new bytes is therefore not an observable state, so a checksum would guard against
something that cannot happen. Both `fsync`s are load-bearing: `rename` makes the *name* change
atomically and says nothing about the bytes reaching disk, and the rename is itself a directory
change that needs flushing. This is one of the few places worth paying for durability, because
unlike an index this file cannot be rebuilt — the configuration is not derivable from the log.

**An unknown version is refused, in both directions.** A config file read *wrongly* is worse than
one not read at all: a misinterpreted retention limit silently deletes data or silently keeps it
forever, and neither reports an error.

**`-1` for unlimited lives only here.** `RetentionPolicy` holds an `optional`, so "unlimited" and
"keep almost nothing" cannot be confused in memory. The sentinel translation happens at this
boundary and nowhere else.

#### Maintenance does two jobs

```cpp
void LogManager::runMaintenance(int64_t nowMs) {
    shared_lock lock(logsMutex_);
    for (auto& [tp, log] : logs_) {
        log->maybeRoll(nowMs);          // ← easy to forget
        log->applyRetention(nowMs);
        log->sweepGraveyard(nowMs);     // added at M3
    }
    lock.unlock();
    sweepDeletedPartitions(nowMs);      // added at M3
}
```

The second line is obvious; the first is the trap from decision 11. If segments roll only during
`append`, an **idle partition never rolls**, so its active segment never seals, so retention
never fires and its data lives forever. Age-based rolling must be driven by the sweeper as well
as by the write path.

**Amended at M3 — four jobs, not two**, the extra pair being the graveyard drains above. Note the
order: rolling comes first because retention can only delete *sealed* segments, so on an idle
partition the roll is what produces something to delete.

**And a race this created.** M2 justified taking no lock in `Log::append` on the grounds that
"a roll happens on this same thread, so the appender cannot race with itself". The sweep calls
`maybeRoll` too, from its own thread, and that ended the argument: two callers could both pass the
`shouldRoll` check and both try to create a segment at the same base offset. `maybeRoll` now holds
the exclusive lock across the decision as well as the swap, and `append` holds a *shared* lock
while using `active_` — shared, despite mutating, because there is exactly one appender and what
the lock excludes is the roll, not other writers. Readers hold the same shared lock and still run
alongside the appender, which is the point.

#### The thread that drives it

**Added at M3.** The sweep waits on a condition variable rather than sleeping, so stopping is
immediate. With a plain sleep, a broker configured to sweep every five minutes takes up to five
minutes to shut down — long enough that whatever supervises it sends `SIGKILL`, and every clean
shutdown becomes a crash-recovery path on the next boot.

A sweep that throws is **counted, not fatal and not swallowed**. Letting it propagate kills the
thread, so one unreadable file stops every partition being maintained, permanently. Swallowing it
silently leaves retention dead with nothing to show for it, which is how a disk fills with nobody
knowing. So `sweepFailures()` is a number an operator can look at.

The destructor stops the thread, in the destructor *body* — which runs before any member is
destroyed, so the join happens while `logs_` is still alive rather than while it is being freed
underneath a sweep in progress.

#### The write path, socket to disk

```
1  network thread   frame request → RequestHeader + body bytes
2                   push onto request queue, go back to polling
                              │
3  I/O thread       pop; parse ProduceRequest{ acks, timeoutMs, [(tp, batchBytes)] }
4                   for each partition:
5                     Log* log = logManager.get(tp)     → not hosted? NOT_LEADER
6                     validate batch CRC                → corrupt? CORRUPT_MESSAGE
7                     log->append(batchBytes)
                              │
8  Log::append        base = logEndOffset_
9                     stamp base offset + leader epoch into the header, in place
10                    active_->shouldRoll()?  → seal + create   (unique_lock)
11                    active_->append(base, bytes)      → write() to the .log fd
12                    bytesSinceIndexEntry_ >= 4096?    → index_.append(base, pos)
13                    logEndOffset_.store(base + recordCount, release)
14                    single-node: highWatermark_ = logEndOffset_
                              │
15 network thread   response {tp → baseOffset} written to socket
```

No `fsync` anywhere — decision 14.

**Line 9 is the only time the broker modifies producer bytes**, and it touches 12 of them. It is
legal because the v2 CRC covers only what follows the CRC field:

```
┌──────────────┬─────────────┬───────────────────────┬───────┬──────┬───────────────────┐
│ baseOffset 8 │ batchLen 4  │ partitionLeaderEpoch 4│ magic1│ crc 4│ attributes … body │
└──────────────┴─────────────┴───────────────────────┴───────┴──────┴───────────────────┘
   ▲                              ▲                                 └── CRC covers this ──┘
   └── broker stamps ─────────────┘
```

Base offset and leader epoch sit **outside** the checksum on purpose, so the broker can assign
them without recomputing a CRC over a possibly-compressed body. That field ordering exists to
protect decision 13.

**Line 13 needs a record count without decompressing**, and takes it from `lastOffsetDelta` in
the header — also outside the compressed body, for exactly this reason. The broker advances its
log end offset knowing only how *many* records arrived, never what they are.

**A correction the trace forced on section 1:** `append` was written as
`span<const uint8_t>`. Line 9 mutates the buffer, so it must be:

```cpp
Offset append(span<uint8_t> batchBytes);   // stamps 12 header bytes in place
```

The `const` version would force a full copy of every batch — precisely the cost this design
exists to avoid.

### 3. The record batch codec

The first Kafka-shaped component. It turns `{key, value, timestamp}` into the exact bytes that
live in a segment — and by decision 13 those same bytes are what `sendfile` puts on a consumer's
socket, so **the on-disk format is the wire format**. Pure library, no I/O, testable in memory.

Three later milestones are constrained by what is decided here: the index stores relative
offsets that only exist because of delta encoding; replication ships raw batch bytes untouched;
and zero-copy fetch works only because the broker can serve a batch it never decompressed.

#### Layout — Kafka v2, byte-exact

```
  ┌──── outside the CRC ─────────────────────────────┐
  │ baseOffset           int64   ← broker stamps     │
  │ batchLength          int32                       │
  │ partitionLeaderEpoch int32   ← broker stamps     │
  │ magic                int8    (= 2)               │
  ├──────────────────────────────────────────────────┤
  │ crc32c               uint32                      │
  ├──── covered by the CRC ──────────────────────────┤
  │ attributes           int16   (compression, flags)│
  │ lastOffsetDelta      int32   ← record count      │
  │ firstTimestamp       int64                       │
  │ maxTimestamp         int64                       │
  │ producerId           int64   ← idempotence (M7)  │
  │ producerEpoch        int16                       │
  │ baseSequence         int32                       │
  │ recordCount          int32                       │
  │ [ records ... ]              ← possibly compressed│
  └──────────────────────────────────────────────────┘

record:  length varint | attributes int8 | timestampDelta varlong | offsetDelta varint
                       | keyLength varint | key | valueLength varint | value
                       | headerCount varint | headers...
```

Three properties, each load-bearing:

1. **Base offset and leader epoch precede the CRC**, so the broker stamps them into a compressed
   batch without recomputing a checksum over the body.
2. **`lastOffsetDelta` and `recordCount` sit outside the compressed body**, so the broker
   advances its log end offset knowing only *how many* records arrived, never what they are.
3. **Deltas, not absolutes**, so a producer can encode before knowing which offsets the broker
   will assign.

#### Fidelity: byte-exact, including fields nothing uses yet

`producerId`, `producerEpoch` and `baseSequence` are written (as `-1`) from M1, and record
headers are parsed and preserved, even though nothing consumes them until M7 and Tier A
respectively.

The alternative — a trimmed format now, extended at M7 — was rejected. The cost is not
compatibility but sequencing: changing the format at M7 would invalidate every `.log` file and
every test fixture, and would mean rewriting the codec, the index and the recovery scan at
exactly the point where replication is already the hard problem. Thirty unused bytes per batch
is cheap insurance, and byte-exactness makes "this is Kafka's v2 format" verifiable by dumping a
file next to real Kafka's.

#### Decode borrows, never owns

A decoded record exposes `span`s into the caller's buffer rather than owning `vector`s. The
broker must never own record bytes; a decoded view is valid only while its buffer lives, and
the CLI copies when it genuinely needs to.

`BatchHeaderView` parses the fixed prefix and stops — the broker's write path needs nothing
else. Full record iteration exists for the CLI, compaction, and tests.

#### Decode errors are exceptions

Unlike the read path, where a lagging or caught-up consumer is routine, a batch that fails its
CRC is genuinely exceptional and surfaces once per recovery rather than once per poll. So decode
throws `CorruptData` and recovery catches it to decide where to truncate.

#### Rejected alternatives

| Alternative | Why not |
|---|---|
| A simpler homegrown batch format | This is the one place decision 19 says to copy Kafka exactly — the byte-level work *is* the education, and the files are then structurally real |
| Fixed `int32` lengths instead of varints | Four 4-byte length fields on a 50-byte message is 32% overhead; deltas are small numbers and small numbers should cost one byte |
| Absolute offsets per record | Forces the broker to decompress and re-encode every batch — the thing the whole design exists to avoid |
| Protobuf / FlatBuffers | External dependency, no control over byte layout, no way to put the CRC boundary where it must go |
| zlib CRC32 instead of CRC32C | Different polynomial, breaks format compatibility, and forgoes the hardware instruction |

#### Banked for later

- **Hardware CRC32C** (`_mm_crc32_u64`) behind a runtime CPU check — M9, with benchmarks to
  justify it. M1 ships the portable table implementation.
- **Compression codecs** (lz4, zstd) — the first external dependencies, deferred until there is
  a broker to measure them on. M1 handles the attribute *flag*, not the algorithm.
- **Producer-side batching** (`linger.ms` / `batch.size`) — client behaviour, not broker
  behaviour; it lands in the CLI at M4 where there is something to send to.
- **Control batches** (transaction markers) — only if transactions ever land.
- **Buffer pooling** — decode allocates nothing, but encode allocates per batch. Wait for M9
  numbers.

### 4. The protocol and the server

The first chunk where dariyakyu becomes a program you talk to rather than a library you link.
Everything below sits *above* the storage layer and knows nothing about segments — a handler asks
`LogManager` for a `Log`, gets a `FileRange` back, and turns it into bytes on a socket.

The protocol is **our own, shaped like Kafka's** (decision 19). Wire compatibility is weeks of
serialisation work for very little learning, so it is scoped out — but the shape is kept close
enough that a translation shim could be added later rather than a redesign. One consequence
worth stating up front: `BufferReader::readArrayLength` already exists, written at M1 with a
comment saying it is for this layer. Nearly every request here is an array of topics containing
an array of partitions.

#### The wire: length-prefixed frames

```
┌─────────────────────────────────────────────┐
│ frameLength   int32   bytes that follow     │
├─────────────────────────────────────────────┤
│ apiKey        int16                         │
│ apiVersion    int16                         │
│ correlationId int32                         │
│ clientId      int16 length + bytes          │
├─────────────────────────────────────────────┤
│ [ body, per apiKey ]                        │
└─────────────────────────────────────────────┘
```

A length prefix rather than a delimiter, because record bytes are arbitrary and any delimiter
would need escaping — which would mean touching the payload, which is the one thing this design
refuses to do.

`frameLength` is bounded by a configured maximum and a frame claiming more is refused before a
single byte is buffered. Without that, one client sending `0x7FFFFFFF` makes the broker try to
allocate two gigabytes.

**`correlationId` is echoed, never interpreted.** It exists so a client can have several requests
in flight on one connection and match responses to them. Note what that does *not* require of the
broker at M4: one thread per connection reads a request, answers it, and reads the next, so
responses come back in order. The id is what lets M9 reorder them without a protocol change.

#### A response is a list of segments, not a buffer

This is the decision the rest of the chunk hangs off, and it is forced by two requirements that
pull in opposite directions.

A `Fetch` response interleaves two kinds of bytes:

```
[ frameLength ][ correlationId ][ meta for orders-0 ][ 900 KB from a .log file ]
                                [ meta for orders-1 ][  40 KB from another    ]
```

The metadata is computed in memory. The record bytes are already in a file, and decision 13 says
they must never enter user space — that is what `sendfile` is for, and `Log::read` returning a
`FileRange` is how the storage layer enforces it.

But `sendfile` can only send *file* bytes. It cannot also send the metadata. So a response cannot
be one buffer, and it cannot be one syscall: it has to be written as an alternating sequence.

And the **frame length has to go out first**, before any of it, which means knowing the total —
including bytes not yet sent — in advance.

That is affordable precisely because `Log::read` resolves a *location*: every range's length is
known without reading it. So:

```cpp
// One piece of a response: bytes we own, or a place in a file.
struct ResponseSegment {
    vector<uint8_t> buffer;   // used when range is empty
    FileRange       range;
};

class Response {
public:
    void append(vector<uint8_t> buffer);
    void append(FileRange range);
    size_t totalBytes() const;            // the frame length, by arithmetic
    span<const ResponseSegment> segments() const;
};
```

and the connection walks it:

```cpp
writeFrameLength(socket, response.totalBytes());
for (const auto& segment : response.segments())
    if (segment.range.empty()) writeAll(socket, segment.buffer);
    else                       sendFileAll(socket, segment.range);
```

Three things fall out of this shape, and each of them is why it is the right one:

1. **Encoders never learn about sockets.** A handler builds a `Response`; something else decides
   how it reaches the wire. The same `Response` is testable in memory by concatenating its
   segments, which is how every codec test in Group B works.
2. **A half-sent response has a small, obvious resumption state** — which segment, and how far
   into it. At M4 with blocking sockets that is a loop; at M9 with non-blocking sockets it is
   exactly the state a partially-written response needs to carry.
3. **Every other API still works.** A `Produce` or `Metadata` response is a `Response` with one
   buffer segment and no ranges. The abstraction costs them nothing.

Kafka arrived at the same answer, and calls it `Send` — with a `MultiRecordsSend` for the
multi-partition case.

**This is what M3's deferred deletion was for.** A response is built on one thread and written
later, possibly after retention has run. Without the graveyard, the descriptor in a `FileRange`
could be closed — or its number reused by another file, sending a consumer someone else's bytes.
M4 is the first milestone that needs that guarantee, and the first that would have discovered its
absence the hard way.

#### Error codes: numeric, and per partition

Errors are `int16` codes, as Kafka does it, because a client has to branch on them and a string
is not something to branch on.

They are attached **per partition**, not per request. A fetch across twelve partitions where one
has been reassigned must return eleven partitions' worth of data plus one error, not fail
wholesale — otherwise a single moved partition stalls every consumer that happened to batch it
with others.

```cpp
enum class ErrorCode : int16_t {
    None                    = 0,
    Unknown                 = 1,
    OffsetOutOfRange        = 2,   // from ReadError::BelowLogStart / AboveLogEnd
    CorruptMessage          = 3,   // a produced batch failing its own CRC
    NotLeaderForPartition   = 4,   // LogManager::get returned nullptr
    UnknownTopicOrPartition = 5,
    TopicAlreadyExists      = 6,
    InvalidTopic            = 7,   // a name that cannot be a directory name
    RequestTimedOut         = 8,
    UnsupportedVersion      = 9,
};
```

**This layer is where storage's vocabulary is translated, and the translation only happens here**
(decision 12). Storage never learns what a wire code is:

| Storage says | Wire says |
|---|---|
| `ReadError::BelowLogStart` | `OffsetOutOfRange` |
| `ReadError::AboveLogEnd` | `OffsetOutOfRange` |
| `LogManager::get` → `nullptr` | `NotLeaderForPartition` |
| `OffsetInvariantViolated` thrown | `Unknown` — it is a broker bug, and saying so plainly beats inventing a client-facing story for it |
| `IoError` thrown | `Unknown` |

**Amended while implementing — `CorruptData` has no single answer, so it gets no
mapping function.** The same exception type means different things depending on which direction
the bytes came from:

- thrown from a **`Produce`** → `CorruptMessage`. The client sent a batch that fails its own
  checksum; that is its fault and it should be told precisely.
- thrown from a **`Fetch`** → `Unknown`. Our files are damaged. Telling the client its *message*
  was corrupt would send it chasing a bug it does not have.

So only `errorCodeFor(ReadError)` exists as a function — the one translation that is unambiguous.
Exceptions are mapped at each handler's catch site, where the direction is known. A universal
`errorCodeFor(const Error&)` would have looked tidier and been wrong.

Note the first two collapse. The distinction matters to the *client's reset policy*, which is why
storage keeps them apart — but Kafka's shape has one code here, and the client learns which side
it fell on by comparing against the log start and end offsets it is also given. Keeping our own
codes distinct is banked below.

#### The five APIs

Enough to produce and consume, and nothing else.

| API | Request | Response |
|---|---|---|
| `Metadata` | topics (empty = all) | per topic: partitions, and which broker leads each |
| `CreateTopic` | name, partition count, config overrides | per topic error code |
| `Produce` | acks, timeoutMs, `[(tp, batchBytes)]` | per partition: error, base offset assigned |
| `Fetch` | maxWaitMs, minBytes, `[(tp, offset, maxBytes)]` | per partition: error, high watermark, log start, records |
| `ListOffsets` | `[(tp, timestamp)]` — earliest / latest | per partition: error, offset |

`Produce` carries **batch bytes the broker does not decode**. It validates the CRC and reads the
header for a record count, then hands the bytes to `Log::append`, which stamps twelve of them.
That is the whole write path, and it is why a compressed batch costs the broker nothing.

`Fetch`'s `maxWaitMs` and `minBytes` are accepted and **ignored** at M4: a fetch returns whatever
is available immediately. Long-polling — parking a fetch until enough data arrives — needs a
request to outlive the thread that read it, which is M9's architecture. Ignoring them keeps the
protocol stable across that change rather than adding fields later.

`ListOffsets` is deliberately first to implement. It touches no record data, so it exercises
framing, dispatch and error mapping with nothing else in the way.

#### Dispatch: `ApiRegistry` and `BrokerContext`

```cpp
// Everything a handler is allowed to touch.
struct BrokerContext {
    LogManager& logs;
    NodeId      nodeId;
    // M8 adds the controller's view; M5 adds the group coordinator.
};

using Handler = function<Response(const RequestHeader&, BufferReader& body, BrokerContext&)>;

class ApiRegistry {
public:
    void registerHandler(ApiKey key, int16_t version, Handler handler);
    Response dispatch(const RequestHeader&, BufferReader& body, BrokerContext&) const;
};
```

A registry rather than a `switch`, for one reason that matters later: `apiVersion` is in the
header, and versioned APIs mean two handlers for one key. A `switch` on key with a nested `switch`
on version is the shape that rots. An unknown key or version answers `UnsupportedVersion` rather
than closing the connection — a client probing what a broker supports is normal behaviour, not an
error.

`BrokerContext` is a struct of references, passed by reference, holding no ownership. Handlers are
stateless; everything they mutate lives in `LogManager`, which already has its own locking. There
is no broker-wide lock in this design and no place for one to be added by accident.

#### Threading: one thread per connection

M4's server is the simplest thing that works:

```
acceptor thread   accept() → spawn a thread → back to accept()
connection thread read frame → dispatch → write response → repeat until EOF
```

Chosen because it makes M4 about the *protocol*, not about an event loop. Every hard concurrency
question — a request queue, an I/O pool, `sendfile` off the network thread, backpressure — is
decision 20's territory and is M9's milestone, with benchmarks to say whether it was worth it. A
thread per connection is fine for the tens of connections this will ever see, and it is
comparable enough to measure M9 against.

**Shutting it down is the part that needs designing.** A thread blocked in `accept()` or `read()`
does not notice a flag. So:

- the acceptor is unblocked by **closing the listening socket**, which makes `accept` fail
- connection threads are unblocked by `shutdown()` on their sockets, then joined
- a thread mid-`sendfile` finishes or fails; either is acceptable, because a client whose broker
  is shutting down has to reconnect regardless

This is the M4 counterpart of M3's condition-variable shutdown, and for the same reason: a
shutdown slow enough to be `SIGKILL`ed turns every deploy into a crash-recovery path.

#### The CLI

`dariyakyu-cli` is a real client, not test scaffolding, and it links the same `protocol` library
the broker does. That is deliberate: a field the encoder writes and the decoder ignores shows up
the first time the CLI talks to the broker, rather than at M5 when a second implementation
appears.

```
dariyakyu-cli produce  <topic> <partition>          records from stdin, one per line
dariyakyu-cli consume  <topic> <partition> [--from earliest|latest|N]
dariyakyu-cli describe <topic>                      Metadata, printed
dariyakyu-cli dump-segment <partition-dir>          the existing dariyakyu-dump, folded in
```

`dump-segment` is the odd one out — it reads files directly and never opens a socket. It stays
because that is exactly when it is wanted: when the broker will not start.

#### Rejected alternatives

| Alternative | Why not |
|---|---|
| A response as one `vector<uint8_t>` | Copies every payload byte twice and deletes decision 13 with it. The reason the segment list exists |
| Kafka wire compatibility | Weeks of serialisation for little learning (decision 19). The shape is kept close so a shim stays possible |
| Text or JSON framing | The payload is arbitrary bytes; any escaping means touching it |
| A delimiter instead of a length | Same problem, plus no way to size a read up front |
| One `switch` on `apiKey` | `apiVersion` makes it a nested switch, which is the shape that rots |
| An event loop at M4 | Makes the milestone about the loop rather than the protocol. Decision 20 and M9's benchmarks |
| Long-polling fetches at M4 | Needs a request to outlive the thread that read it — M9's architecture. The fields are accepted now so the protocol does not change later |
| A broker-wide lock | Nothing needs one: handlers are stateless and `LogManager` locks its own registry |

#### Banked for later

- **`sf_hdtr`** — macOS's `sendfile` can send in-memory headers and file bytes in one syscall,
  which maps exactly onto the segment list. Linux has no equivalent. M9, with numbers.
- **Long-polling fetch** (`maxWaitMs`, `minBytes` honoured) — M9, when a parked request has
  somewhere to live.
- **Distinct codes for the two `OffsetOutOfRange` cases** — storage already separates them, and a
  client's reset policy genuinely differs. Deferred only to keep the first version Kafka-shaped.
- **`acks` beyond a single node** — the field is in `Produce` from M4 and means nothing until M7.
- **Multi-broker `Metadata`** — one node, so every partition's leader is this one. M8 makes it a
  real answer.
- **TLS, SASL, quotas** — not in this project.
