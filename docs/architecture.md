# Cascade architecture

I keep this document close to the code so the diagrams describe the current implementation rather than a future target.

## Internal request path

```mermaid
flowchart TB
    Clients["Kafka clients<br/>Java · Go · Python · JavaScript · .NET"]
    Frame["Kafka framing and schema decoding"]
    Session["ConnectionSession<br/>TLS · SASL · principal · limits"]
    Admission["Admission and authorization<br/>frame bound · request gate · quota · ACL · audit"]
    Registry["ApiHandlerRegistry"]
    Produce["ProduceApiHandler"]
    Fetch["FetchApiHandler"]
    Metadata["MetadataApiHandler"]
    Groups["GroupApiHandler"]
    Transactions["TransactionApiHandler"]
    Admin["AdminApiHandler"]
    Security["SecurityApiHandler"]
    PartitionLog["PartitionLog<br/>segments · indexes · recovery · compaction"]
    GroupCoordinator["GroupCoordinator<br/>membership · assignment · offsets"]
    DeliveryCoordinator["DeliveryCoordinator<br/>producer IDs · epochs · transactions"]
    ClusterManager["ClusterManager orchestration"]
    Policies["BrokerMembership · ControllerElection<br/>CoordinatorRouter · PartitionLeadership"]
    Replication["ReplicationManager<br/>ISR append · recovery · truncation"]
    Quorum["Metadata and coordinator quorums"]

    Clients --> Frame --> Session --> Admission --> Registry
    Registry --> Produce --> PartitionLog
    Registry --> Fetch --> PartitionLog
    Registry --> Metadata --> ClusterManager
    Registry --> Groups --> GroupCoordinator
    Registry --> Transactions --> DeliveryCoordinator
    Registry --> Admin --> ClusterManager
    Registry --> Security
    PartitionLog <--> Replication
    GroupCoordinator <--> Quorum
    DeliveryCoordinator <--> Quorum
    ClusterManager --> Policies
    ClusterManager <--> Quorum
    ClusterManager <--> Replication
```

The handler registry owns API-key routing. Domain handlers expose a narrow dispatch contract; protocol methods still share one broker context so authentication, authorization, metadata, and acknowledgement decisions use the same request/session state.

`ClusterManager` remains the lifecycle facade for the cluster, while deterministic policy is split into independently tested membership, election, routing, and partition-leadership components. Network replication remains in `ReplicationManager`; coordinator persistence and voting live under `cascade.coordinator`.

## Three-node replication and leader failover

```mermaid
flowchart LR
    Producer["Idempotent producer<br/>acks=all"]
    Consumer["read_committed consumer"]
    B1["Broker 1<br/>partition leader"]
    B2["Broker 2<br/>ISR follower"]
    B3["Broker 3<br/>ISR follower"]
    Meta["Controller quorum<br/>leader epoch + ISR"]
    HWM["Committed high watermark"]

    Producer -->|Produce| B1
    B1 -->|replica append| B2
    B1 -->|replica append| B3
    B2 -->|ack| B1
    B3 -->|ack| B1
    B1 --> HWM --> Producer
    HWM --> Consumer
    Meta -.-> B1
    Meta -.-> B2
    Meta -.-> B3
```

```mermaid
sequenceDiagram
    participant P as Kafka producer
    participant B1 as Broker 1 / old leader
    participant B2 as Broker 2 / new leader
    participant B3 as Broker 3 / ISR
    participant Q as Controller quorum

    P->>B1: Produce(sequence n, acks=all)
    B1->>B2: ReplicaAppend
    B1->>B3: ReplicaAppend
    B2-->>B1: appended
    B3-->>B1: appended
    B1-->>P: acknowledged
    Note over B1: process is killed
    Q->>Q: lose lease / commit failure
    Q->>B2: promote ISR member, leader epoch + 1
    P->>B2: retry with same producer identity
    B2->>B3: ReplicaAppend
    B3-->>B2: appended
    B2-->>P: acknowledged
```

An `acks=all` response is sent only after the configured minimum ISR is present and every current ISR member appends. The committed high watermark advances separately from local log end. After failure, only an in-sync replica can be promoted; the new epoch fences stale leaders. Returning replicas compare offsets and truncate divergent data before ISR admission.

## Durable state ownership

| State | Owner | Durability boundary |
| --- | --- | --- |
| Record batches | `PartitionLog` | Segment append plus configured force policy |
| Committed offsets and group membership | Group coordinator shards | Majority-forced shard quorum records |
| Producer epochs, transactions, outcomes | Delivery coordinator shards | Majority-forced shard quorum records |
| Topics, replicas, ISR, leaders, membership | Metadata quorum | Majority metadata commit |
| Controller vote and term | `ControllerStateStore` | Forced CRC32C journal |
| High watermarks | Partition checkpoint | Double-buffered checksum-protected checkpoint |

## Important boundaries

- Public Kafka traffic and authenticated peer RPCs use different API ranges and authorization paths.
- Immutable acknowledged views serve reads while mutations prepare and commit.
- Group and transaction shards lock in sorted order for atomic multi-shard changes.
- Quota-limited requests reserve against the active controller's fenced cluster ledger; zero limits bypass that path.
- Storage acknowledgements, replication acknowledgements, and physical disk forcing are distinct concepts and are documented separately.

