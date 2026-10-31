# Platform architecture

The platform keeps batch ingestion, the Kafka demonstration stream, quality checks, and existing internal sample data distinct while using PostgreSQL for durable analytical storage.

```mermaid
flowchart TB
  subgraph Sources[Data sources]
    I[(Existing internal synthetic<br/>transactional and activity tables)]
    D[DummyJSON public REST API<br/>external sample records]
    S[Configurable event generator<br/>simulated e-commerce activity]
  end

  subgraph Processing[Python data platform]
    B[Batch ingestion command<br/>pagination, validation, retries]
    P[Kafka producer<br/>UUID key, acks all]
    C[Kafka consumer<br/>validate, deduplicate, measure]
    Q[Read-only data quality checks<br/>configurable stale threshold]
  end

  K[(Kafka topic<br/>3 partitions)]

  subgraph PostgreSQL[PostgreSQL]
    CORE[(Existing internal tables<br/>and analytical views)]
    API[(api_* staging tables<br/>JSONB source records)]
    EVT[(simulated_events<br/>unique UUID event IDs)]
    OBS[(streaming batch and<br/>consumer error history)]
    RUNS[(API ingestion runs)]
    QUAL[(quality runs and results)]
  end

  G[Grafana<br/>5 operational dashboards<br/>+ Data Quality dashboard]

  I --> CORE
  D --> B --> API
  B --> RUNS
  S --> P --> K --> C --> EVT
  C --> OBS
  CORE --> Q
  API --> Q
  EVT --> Q
  Q --> QUAL
  CORE --> G
  API --> G
  EVT --> G
  OBS --> G
  RUNS --> G
  QUAL --> G

  classDef internal fill:#e8f1ff,stroke:#2864a5,color:#12263a
  classDef external fill:#fff2d8,stroke:#b7791f,color:#3b2a10
  classDef simulated fill:#e8f7ef,stroke:#24804b,color:#123321
  classDef store fill:#f2ebff,stroke:#7040a0,color:#28133d
  class I,CORE internal
  class D,API external
  class S,P,K,C,EVT,OBS simulated
  class RUNS,QUAL store
```

## Processing and delivery behavior

The API command retrieves paginated endpoints, validates IDs and pagination metadata, and retries transient HTTP errors. Loaders upsert on the DummyJSON source key and record a run per endpoint. The stream producer emits a configurable event rate. The consumer validates event structure, writes event rows and batch telemetry transactionally, and commits Kafka offsets only after PostgreSQL has committed. A PostgreSQL UUID primary key prevents replayed messages from creating duplicate event records. The quality command reads its source tables, writes one result for each check, and does not repair or update source records.

The system-event seed data, internal commerce tables, API sample tables, and generated Kafka records have separate labels and storage boundaries. None of the API samples or simulated events are presented as actual platform customers or orders.
