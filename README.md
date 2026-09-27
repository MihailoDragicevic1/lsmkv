# lsmkv

A durable, Dynamo-inspired distributed key-value store implemented in Go, combining a crash-safe **LSM storage engine** with a **replicated quorum-based distributed layer**.

The project explores the core building blocks behind modern distributed storage systems: write-ahead logging, memtables, SSTables, compaction, consistent hashing, replication, quorum reads/writes, failure handling, and crash recovery.

## Overview

`lsmkv` consists of two main layers:

- **Local LSM Storage Engine** — persistent storage built around WAL, memtables, SSTables, Bloom filters, manifests, background flushing, and size-tiered compaction.
- **Distributed Layer** — multiple nodes communicating through gRPC, using consistent hashing, replication, coordinator routing, and configurable `N/R/W` quorum operations.

The current distributed implementation supports a functional three-node cluster configured with:

```text
Replication Factor (N) = 3
Read Quorum       (R) = 2
Write Quorum      (W) = 2
```

With this configuration, the cluster can continue serving basic operations while one replica is unavailable, as long as the required quorum remains reachable.

> This project is inspired by Dynamo-style distributed systems. It is intended as an educational and engineering implementation of their core concepts rather than a production replacement for systems such as DynamoDB or Cassandra.

---

## Features

### Local LSM Storage Engine

- `Put`, `Get`, and `Delete` operations
- Overwrite semantics
- Tombstone-based local deletion
- Write-Ahead Log before memtable writes
- Configurable WAL `fsync` durability
- WAL segmentation and checksum validation
- WAL replay during restart
- Recovery from truncated or torn WAL tails
- Active and immutable memtables
- Background flushing
- SSTable writer and reader
- Bloom filters for SSTable lookup optimization
- Manifest-based tracking of live SSTables
- Immutable published version views
- Size-tiered compaction
- Graceful and fast shutdown paths
- Orphan SSTable protection
- Crash protection between SSTable rename and manifest publication
- Defensive validation of WAL, manifest, SSTable index, and Bloom filter metadata
- Hard-crash tests for acknowledged `Put` and `Delete` operations

### Distributed Quorum Layer

- gRPC client-facing and node-to-node communication
- Static cluster configuration
- Consistent hashing with virtual nodes
- Deterministic coordinator selection
- Preference lists for replica placement
- Request forwarding to the responsible coordinator
- Configurable replication factor (`N`)
- Configurable read quorum (`R`)
- Configurable write quorum (`W`)
- Parallel replication
- Quorum `Put`
- Quorum `Get`
- Quorum `Delete`
- Early return once the required quorum is reached
- Three-node integration testing
- Read/write/delete operation with one unavailable replica

---

## Architecture

```text
                         Client
                           |
                           v
                 Any Cluster Node
                      via gRPC
                           |
                           v
               Is this the coordinator?
                    /             \
                  yes              no
                   |                |
                   |                v
                   |         Forward Request
                   |                |
                   +--------+-------+
                            |
                            v
                 Consistent Hash Ring
                  / Preference List
                            |
             +--------------+--------------+
             |              |              |
             v              v              v
         Replica 1      Replica 2      Replica 3
             |              |              |
             v              v              v
        Local LSM       Local LSM       Local LSM
          Store           Store           Store
```

Each cluster node contains its own local LSM storage engine and a distributed coordination layer.

The storage engine exposes only the fundamental operations:

```text
Get(key)
Put(key, value)
Delete(key)
```

It has no knowledge of cluster membership, replication, consistent hashing, or quorum rules. Those responsibilities remain isolated within the distributed layer.

---

## How It Works

### Coordinator & Replica Selection

For every key:

1. The key is hashed onto the consistent-hashing ring.
2. The first node clockwise from the hash becomes the coordinator.
3. The coordinator and the following `N - 1` nodes form the preference list.
4. A client request may be sent to any cluster node.
5. If that node is not the coordinator, the request is forwarded.
6. The coordinator contacts replicas in parallel.
7. The operation returns once the required read or write quorum is reached.

```text
key
 |
 v
hash(key)
 |
 v
coordinator
 |
 v
preference list
 |
 v
parallel replica requests
 |
 v
R or W quorum reached
```

### Write Path

A distributed write follows:

```text
Client Put
    |
    v
Any Cluster Node
    |
    v
Coordinator
    |
    v
Preference List
    |
    +------> Replica 1
    +------> Replica 2
    +------> Replica 3
                 |
                 v
            Local LSM Store
                 |
                 v
                WAL
                 |
                 v
              Memtable
                 |
                 v
         Background Flush
                 |
                 v
              SSTable
```

At the local storage level, durability follows:

```text
Put / Delete
     |
     v
WAL Append
     |
     v
WAL fsync
     |
     v
Active Memtable
     |
     v
Immutable Memtable
     |
     v
Background Flush
     |
     v
SSTable
     |
     v
Manifest Publish
     |
     v
New Version
     |
     v
Background Compaction
```

The WAL is written before the mutation becomes part of the memtable, allowing acknowledged operations to be recovered after a process crash according to the configured `fsync` policy.

### Read Path

Local reads search storage from newest to oldest:

```text
Get(key)
   |
   v
Active Memtable
   |
   v
Immutable Memtables
   |
   v
Current SSTable Version
```

A tombstone found in a newer layer prevents an older value from becoming visible.

---

## Quorum Model

The default three-node configuration uses:

```text
N = 3
R = 2
W = 2
```

Therefore:

```text
R + W > N
2 + 2 > 3
```

The read and write quorums overlap in at least one replica.

In practice:

- `Put` succeeds after two replicas acknowledge the write.
- `Get` succeeds after two replicas respond.
- `Delete` succeeds after two replicas acknowledge the deletion.
- One node may be unavailable while basic operations continue if the remaining quorum is reachable.
- The coordinator does not wait for an unavailable third replica after the required quorum has already been achieved.

---

## WAL & Crash Recovery

The Write-Ahead Log is the first durability layer of the local storage engine.

It provides:

- WAL segment creation and reopening
- Segment rolling
- Binary record encoding and decoding
- Checksums
- Existing-segment header validation
- Replay during restart
- Recovery from incomplete final records
- Safe truncation of torn WAL tails

With:

```text
WALFsyncEveryN = 1
```

acknowledged local `Put` and `Delete` operations are covered by subprocess crash tests that verify WAL-based recovery after abrupt process termination without calling `Close()`.

The guarantee is limited to process-level crash recovery under the configured `fsync` policy and does not attempt to provide guarantees beyond those offered by the underlying operating system, filesystem, and storage device.

---

## SSTables, Manifest & Compaction

The manifest acts as the authoritative source for live SSTable files.

A newly written SSTable becomes part of active storage only after its metadata has been successfully published through a new manifest. This protects the read state from crashes occurring between SSTable creation and manifest publication.

`lsmkv` currently uses **size-tiered compaction**:

1. Compatible SSTables are selected.
2. Entries are merged according to freshness.
3. A new SSTable is written.
4. The new state is published through the manifest.
5. Replaced tables are removed from the active version view.

SSTable readers also validate index blocks and Bloom filter metadata to prevent corrupted files from producing invalid lookups or runtime failures.

---

## Project Structure

```text
lsmkv/
├── cmd/
│   ├── client/             # Client entrypoint
│   ├── lsmkv/              # Local storage CLI
│   └── server/             # Cluster node / gRPC server
│
├── config/                 # Storage and cluster configurations
│
├── internal/
│   ├── coordinator/        # Coordinator and quorum logic
│   ├── lsm/                # Local LSM storage engine
│   ├── node/               # gRPC, forwarding and replication
│   └── ring/               # Consistent hashing and preference lists
│
├── proto/                  # Protocol Buffer and generated gRPC code
├── go.mod
├── go.sum
└── README.md
```

### Main Packages

| Package | Responsibility |
|---|---|
| `internal/lsm` | WAL, memtables, SSTables, manifest, flush, compaction, and recovery |
| `internal/ring` | Consistent hash ring, virtual nodes, and preference lists |
| `internal/coordinator` | Coordinator selection and quorum configuration |
| `internal/node` | gRPC communication, forwarding, replication, and local storage integration |
| `proto` | RPC contracts and generated gRPC code |
| `cmd/server` | Cluster node entrypoint |
| `cmd/client` | Distributed client entrypoint |
| `cmd/lsmkv` | Local storage CLI |

---
## Getting Started

### Prerequisites

- Go 1.26 or later
- Git

Verify your installation:

```bash
go version
git --version
```

### Clone the Repository

```bash
git clone https://github.com/MihailoDragicevic1/lsmkv.git
cd lsmkv
```

### Run the Test Suite

```bash
go test ./...
```

### Run a Three-Node Cluster

The repository includes configuration files for a local three-node cluster using:

```text
Node 1: 127.0.0.1:18081
Node 2: 127.0.0.1:18082
Node 3: 127.0.0.1:18083
```

Start each node in a separate terminal.

**Terminal 1**

```bash
go run ./cmd/server --config config/node1.json
```

**Terminal 2**

```bash
go run ./cmd/server --config config/node2.json
```

**Terminal 3**

```bash
go run ./cmd/server --config config/node3.json
```

### Use the Client

With the cluster running, open another terminal.

Write a value through Node 1:

```bash
go run ./cmd/client put --addr 127.0.0.1:18081 --key hello --value world
```

Read it through Node 2:

```bash
go run ./cmd/client get --addr 127.0.0.1:18082 --key hello
```

Delete it through Node 3:

```bash
go run ./cmd/client delete --addr 127.0.0.1:18083 --key hello
```

Requests can be sent to any cluster node. If the receiving node is not responsible for coordinating the key, the request is forwarded to the appropriate coordinator.

### Local Storage CLI

The local LSM storage engine can also be used independently of the distributed cluster:

```bash
go run ./cmd/lsmkv --help
```

Available operations include `put`, `get`, `del`, `flush`, `compact`, storage statistics, manifest inspection, and several crash-recovery and storage-engine demonstrations.
## Testing

Run the complete test suite from the project root:

```bash
go test -count=1 ./...
```

Or use:

```bash
go test ./...
```

The test suite covers both the local storage engine and distributed layer.

### Storage Engine

Tests cover:

- WAL encoding and decoding
- WAL segment lifecycle
- WAL replay and torn-tail recovery
- Store operations
- Read paths
- Flush and force-flush behavior
- Backpressure
- SSTable reading and writing
- Bloom filter validation
- Manifest validation
- Compaction
- Background scheduling
- Crash injection
- Hard-crash durability for `Put` and `Delete`

### Distributed Layer

Tests cover:

- Consistent hashing
- Preference-list selection
- Coordinator routing
- gRPC request forwarding
- Three-node replication
- Quorum reads and writes
- Replicated deletes
- Read quorum with one unavailable replica
- Write quorum with one unavailable replica
- Delete quorum with one unavailable replica

---

## Failure Scenarios

For the default `N = 3`, `R = 2`, `W = 2` configuration:

| Scenario | Behavior |
|---|---|
| All replicas available | `Put`, `Get`, and `Delete` execute through the normal quorum flow |
| One replica unavailable during `Put` | Write can succeed using two available acknowledgements |
| One replica unavailable during `Get` | Read can succeed using two available responses |
| One replica unavailable during `Delete` | Delete can succeed using two available acknowledgements |
| Local process crashes after an acknowledged mutation | WAL replay restores acknowledged local state |

---

## Current Limitations

`lsmkv` is a functional distributed storage MVP and intentionally does not attempt to implement the full feature set of production systems such as Dynamo or Cassandra.

The following features are outside the current scope:

- Dynamic cluster membership
- Gossip-based membership and failure detection
- Automatic data migration and rebalancing
- Sloppy quorum
- Hinted handoff
- Read repair
- Anti-entropy repair
- Merkle trees
- Vector clocks
- Conflict resolution for concurrent writes
- Distributed tombstones
- Replica catch-up after recovery
- TLS, authentication, and authorization
- Production metrics and distributed tracing
- Continuous chaos testing

### Distributed Delete Limitation

Local deletion uses tombstones and is crash-safe.

At the distributed level, however, the current implementation does not yet include distributed tombstones, hinted handoff, or repair.

If one replica is unavailable during a successful quorum delete, that replica may retain an older value. The current system does not automatically repair that stale replica when it rejoins the cluster.

Production systems typically address this class of problem using mechanisms such as distributed tombstones, versioning, hinted handoff, and anti-entropy repair.

---

## Tech Stack

- **Go**
- **gRPC**
- **Protocol Buffers**
- **LSM Trees**
- **Write-Ahead Logging**
- **SSTables**
- **Bloom Filters**
- **Consistent Hashing**
- **Quorum Replication**

---

## Future Improvements

Potential extensions include:

- Hinted handoff and sloppy quorum
- Read repair
- Anti-entropy synchronization using Merkle trees
- Gossip-based membership and failure detection
- Vector clocks and conflict resolution
- Dynamic node membership and automatic rebalancing
- Distributed tombstones and replica repair
- Metrics and distributed tracing
- Extended chaos and failure testing

---

## Author

**Mihailo Dragićević**

Computer Science student at the Faculty of Computing (RAF), Union University in Belgrade.

[GitHub](https://github.com/MihailoDragicevic1) · [LinkedIn](https://www.linkedin.com/in/mihailo-dragicevic-3aaa81308/)