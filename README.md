# payment-gateway-evtsrc

An event-sourced, self-hosted multi-bank Virtual Account (VA) payment gateway for Indonesian institutions, built on **Apache Kafka Streams**, **embedded RocksDB state stores**, and a **PostgreSQL 18 CQRS projection sink**.

For local development and starter environments, this repository runs a **single-node deployment** (1 App Instance, 1 Apache Kafka broker, 1 PostgreSQL 18 container) to maintain **1:1 infrastructure parity** with the single-node deployment of the relational [`payment-gateway`](https://github.com/artivisi/payment-gateway) implementation for direct benchmark comparison.

---

## 1. Problem & Architectural Motivation

Institutions collecting payments via Virtual Accounts (universities, hospitals, foundations) require high availability, sub-millisecond callback validation, and single-debt guarantees across multiple bank VAs.

The original [`payment-gateway`](https://github.com/artivisi/payment-gateway) project uses a traditional relational database (PostgreSQL 18) for write transactions and read queries. In high-volume payment bursts, database lock contention (`SELECT FOR UPDATE`), shared connection pools, and IO spikes can degrade bank callback SLAs.

To evaluate a fully decoupled alternative, **`payment-gateway-evtsrc` implements a hybrid Event Sourcing & CQRS architecture**:
1. **Kafka is the Source of Truth (Write Path)**: All domain actions (charge creation, VA allocation, payment callback, settlement) are appended as immutable events into Kafka.
2. **RocksDB Hot-Path Validation**: Kafka Streams topologies maintain local, partitioned **RocksDB state stores** (`KTable`, `ReadOnlyKeyValueStore`) running directly inside the JVM process off-heap. Ingress bank callbacks validate idempotency and single-debt rules in $<1\text{ms}$ without touching an external database.
3. **PostgreSQL 18 CQRS Reporting Sink (Read Path)**: An asynchronous projection sink streams events from Kafka into PostgreSQL 18. The Web UI fetches reporting data, transaction history, audit logs, and reconciliation status from PostgreSQL via **Spring Data JPA**.

---

## 2. Feature List

### 2.1 Multi-Bank Virtual Account Collection (Unified API)
- **Single API integration** for client applications (e.g. academic systems, hospital management, or subledgers like `account-receivable`).
- **Bank-specific protocol adapters**:
  - Maybank (SNAP / REST)
  - BSI (proprietary REST / JSON)
  - CIMB (proprietary SOAP / XML)
- **VA Hosting Models**:
  - **Gateway-hosted (Model 1)**: Gateway resolves VA inquiries in real time against its local RocksDB registry and receives payment callbacks.
  - **Bank-hosted (Model 2)**: Gateway registers VAs at banks upfront and handles payment callbacks.

### 2.2 Flexible Charge Types & Sibling Virtual Accounts
- **Open Charges**: Persistent, variable amount, accepts repeated payments.
- **Closed Charges**: Fixed amount, single payment, closes upon full settlement.
- **Installment Charges**: Multiple partial payments accumulating up to a target debt amount.
- **Sibling Virtual Accounts**: A single charge is payable through multiple bank VAs simultaneously.
- **Single-Debt Invariant**:
  - Payment on one bank sibling automatically updates charge balance in RocksDB and cancels/adjusts all sibling VAs.
  - **Double-Settlement Discrepancy Prevention**: Concurrent payments at two banks trigger a `DoubleSettlementDetectedEvent` for out-of-band refund handling rather than silently double-crediting.

### 2.3 Kafka Streams & RocksDB State Engine (Hot Path)
- **Kafka Event Topics**:
  - `charge-events`: `ChargeCreated`, `ChargeCancelled`, `ChargeCompleted`
  - `va-events`: `SiblingVaRegistered`, `SiblingVaStatusUpdated`
  - `payment-events`: `PaymentReceived`, `DoubleSettlementDetected`
  - `reconciliation-events`: `ReconciliationImported`, `DiscrepancyFlagged`
  - `webhook-events`: `WebhookDispatched`, `WebhookFailed`
- **Embedded RocksDB Stores**:
  - `charge-state-store`: Partitioned RocksDB `KeyValueStore` holding charge metadata, active balance, and state.
  - `va-registry-store`: High-speed RocksDB index mapping VA numbers to charge IDs for $<1\text{ms}$ lookup during bank callbacks.
  - `idempotency-store`: Off-heap RocksDB index storing bank reference numbers to block duplicate callbacks locally.

### 2.4 PostgreSQL 18 Projection Sink & Spring Data JPA (Read Path)
- **Asynchronous Projection Sink**: Kafka consumer group processes domain events using idempotent batch upserts into PostgreSQL 18 reporting schema.
- **Spring Data JPA Web UI**: Thymeleaf + HTMX operator dashboard queries PostgreSQL 18 via Spring Data JPA repositories for rich filtering, pagination, transaction search, and audit trails.

### 2.5 Reconciliation & Discrepancy Management
- End-of-Day (EOD) bank settlement CSV import processor.
- Stateful cross-check matching against recorded payment events in PostgreSQL and RocksDB.
- Automated flagging of unmatched credits, duplicate payments, and amount mismatches.

### 2.6 Resilient Webhook Delivery
- Asynchronous worker consuming `payment-events` to deliver signed webhooks to client applications.
- Exponential backoff retries with per-consumer isolation.

---

## 3. Architecture Design

### 3.1 Paradigm Shift: Kafka Streams + RocksDB vs. Traditional RDBMS

To understand why this project uses an Event Sourcing & CQRS pattern, it is helpful to contrast how state and processing responsibilities are divided in Kafka Streams + RocksDB versus a traditional RDBMS:

| Dimension | Application Node (`streams-engine`) | Kafka Broker Cluster | Traditional RDBMS (`payment-gateway`) |
|---|---|---|---|
| **What Runs Here?** | Spring Boot JVM + Embedded RocksDB C++ Library (`rocksdbjni`). | Apache Kafka Broker Daemon (KRaft). | Monolithic Spring Boot App + PostgreSQL 18. |
| **Where is State Stored?** | **Local Disk / SSD** of the App container (`/var/data/rocksdb`). | **Kafka Segment Logs** on Broker Disk. | **PostgreSQL Data Tables** on DB Disk. |
| **Primary Purpose** | **Sub-millisecond Local Key Lookups** ($<1\text{ms}$) during hot-path callbacks. | **Immutable Event Store**, Streaming, & **Changelog Backup**. | **ACID Transactions**, State Storage, & Read Queries (Shared DB). |
| **Network Hop on Callback** | **Zero Network Hop** (Local RAM / SSD). | **1 Async Append Hop** to Kafka Event Store. | **1–4 Network Hops** (RDBMS SQL Round-Trips). |
| **What Happens on App Crash?** | App container restarts & re-hydrates RocksDB from Kafka **Changelog Topic**. | Unaffected. Keeps serving topics & changelogs. | Application restarts; DB remains single point of failure if unclustered. |

---

### 3.2 Storage Strategy & Location (Where Everything Resides)

| Store Name / Component | Storage Engine | Purpose & Access Pattern |
|---|---|---|
| **Event Store (Write)** | Apache Kafka | Immutable source of truth log for all domain events. |
| **`charge-state-store`** | RocksDB (`KeyValueStore`) | Holds aggregate charge lifecycle state, payment history, and balance indexed by `charge_id`. |
| **`va-registry-store`** | RocksDB (`KeyValueStore`) | Maps VA numbers (`bank_code + va_number`) to `charge_id` for $<1\text{ms}$ lookup during bank callbacks. |
| **`idempotency-store`** | RocksDB (`KeyValueStore`) | Tracks bank payment transaction reference IDs off-heap to prevent duplicate callback processing locally. |
| **Reporting Read DB** | **PostgreSQL 18** | CQRS projection sink storing relational read models for the Web UI, accessed via **Spring Data JPA**. |

```mermaid
flowchart TD
    subgraph App_Server ["Application Server / Container (Spring Boot JVM)"]
        KSTREAM["Kafka Streams Topology (JVM)"]
        subgraph Local_Storage ["Local Disk & Off-Heap Memory"]
            ROCKS[("Embedded RocksDB State Stores<br/>(/var/data/rocksdb)")]
        end
        KSTREAM -->|"Embedded JNI Writes (<1ms)"| ROCKS
    end

    subgraph Kafka_Cluster ["Kafka Broker Cluster"]
        TOPICS["Domain Event Topics<br/>(charge-events, payment-events)"]
        CHANGELOGS["State Store Changelog Topics<br/>(Backup & Recovery Log)"]
    end

    KSTREAM -->|1. Appends Domain Event| TOPICS
    KSTREAM -.->|2. Async Changelog Stream| CHANGELOGS
```

#### Key Storage Principles:
- **RocksDB Resides on the Application Node**: RocksDB runs **inside the JVM process of the Spring Boot application instance via JNI**, persisting data files to the local disk/SSD of the application container (`/var/data/rocksdb`). The Kafka Broker does **not** host or run RocksDB.
- **Zero Network Hop in Hot Path**: Hot-path lookups (idempotency, VA resolution) access local RAM/SSD in **$<1\text{ms}$** without calling an external DB or broker over the network.
- **Broker-Side Backup & Recovery**: Every state mutation written to local RocksDB is automatically streamed to a hidden Kafka **Changelog Topic** on the broker. If an app container crashes or moves, a new container re-hydrates its local RocksDB store automatically from Kafka.

---

### 3.3 Execution & Data Flow (Sequence Diagrams)

#### 1. Event-Sourced & CQRS Architecture Flow (`payment-gateway-evtsrc`)

The hot path completes synchronously in **$<1\text{ms}$** upon writing to Kafka. State processing in RocksDB and relational projections in PostgreSQL 18 execute asynchronously in parallel.

```mermaid
sequenceDiagram
    autonumber
    actor Client as Client / Bank
    participant API as Ingress Gateway
    participant CMD as Command Engine
    participant Kafka as Kafka Event Store
    participant KStream as Kafka Streams (RocksDB)
    participant Sink as Postgres Projection Sink
    participant PG as PostgreSQL 18 DB
    actor Operator as Web UI Operator

    %% Write Path (Synchronous Hot Path)
    rect rgb(235, 245, 255)
    note right of Client: 1. Synchronous Write / Command Path (Hot Path)
    Client->>API: POST /api/v1/payments (Callback / Command)
    API->>CMD: Validate Idempotency & Invariants
    CMD->>Kafka: Append PaymentReceivedEvent
    Kafka-->>CMD: ACK Event Appended
    CMD-->>API: Command Accepted
    API-->>Client: HTTP 200 OK / 201 Created (<1ms Response)
    end

    %% Asynchronous Processing & Projection Path
    rect rgb(240, 255, 240)
    note right of Kafka: 2. Asynchronous Hot-Path & CQRS Projection Path
    par Hot-Path State Update
        Kafka-->>KStream: Stream PaymentReceivedEvent
        KStream->>KStream: Update Charge Balance & Sibling VAs (RocksDB)
    and Asynchronous CQRS Projection
        Kafka-->>Sink: Stream PaymentReceivedEvent
        Sink->>PG: Batch Upsert Read Models (Spring Data JPA)
    end
    end

    %% Read Path
    rect rgb(255, 245, 235)
    note right of Operator: 3. Decoupled Read Path
    Operator->>PG: Query Dashboard & Reports (Spring Data JPA)
    PG-->>Operator: Rendered Views (Thymeleaf / HTMX)
    end
```

#### 2. Traditional RDBMS Architecture Flow (Original `payment-gateway`)

In the original relational implementation, the bank callback thread must execute multiple sequential database queries and writes inside a single blocking ACID transaction before responding to the bank.

```mermaid
sequenceDiagram
    autonumber
    actor Client as Client / Bank
    participant API as Ingress Gateway / Controller
    participant PG as PostgreSQL 18 DB
    actor Operator as Web UI Operator

    %% Synchronous Blocking RDBMS Transaction Path
    rect rgb(255, 235, 235)
    note right of Client: 1. Synchronous Tightly-Coupled RDBMS Transaction
    Client->>API: POST /api/v1/payments (Callback / Command)
    API->>PG: BEGIN TRANSACTION
    API->>PG: SELECT FOR UPDATE (Check Idempotency)
    PG-->>API: Result OK
    API->>PG: SELECT (Check Sibling VA & Balance)
    PG-->>API: Result Active Debt
    API->>PG: UPDATE charge SET balance = ..., status = ...
    API->>PG: INSERT INTO payment (...)
    API->>PG: COMMIT TRANSACTION
    PG-->>API: Transaction Committed
    API-->>Client: HTTP 200 OK (5–50ms Dependent on DB Load)
    end

    %% Read Path (Shares DB IO and Connection Pool)
    rect rgb(255, 245, 235)
    note right of Operator: 2. Read Path (Shared Database Connection Pool)
    Operator->>PG: SELECT * FROM payment WHERE ... (Spring Data JPA)
    PG-->>Operator: Rendered Views (Shares IO with Callback Thread)
    end
```

#### Key Architecture Trade-Off Comparison

| Metric / Dimension | Traditional RDBMS (`payment-gateway`) | Event-Sourced CQRS (`payment-gateway-evtsrc`) |
|---|---|---|
| **Hot-Path Response Time** | $5\text{–}50\text{ ms}$ (blocking DB transaction) | **$<1\text{ ms}$** (instant Kafka event append) |
| **Write-Read Coupling** | Shared DB connections & IO contention | **Fully Decoupled** (Writes hit Kafka/RocksDB; Reads hit Postgres) |
| **Resilience to DB Downtime** | Bank callbacks **fail** if Postgres is down | Bank callbacks **succeed**; Postgres sink catches up asynchronously |
| **Auditability & Replay** | Destructive state updates (`UPDATE`) | **100% Replayable** from Kafka event log genesis |

---

### 3.4 System Architecture Comparison

#### 1. Event-Sourced CQRS System Architecture (`payment-gateway-evtsrc`)

Decoupled event streaming architecture where Kafka is the source of truth, RocksDB provides off-heap $<1\text{ms}$ hot-path validation, and PostgreSQL 18 serves as an asynchronous CQRS reporting sink.

```mermaid
flowchart TD
    subgraph Clients & Banks
        CLIENT[Client Application / Subledger<br/>e.g. account-receivable]
        B1[Maybank<br/>SNAP / REST]
        B2[BSI<br/>REST / JSON]
        B3[CIMB<br/>SOAP / XML]
    end

    subgraph Ingress Gateway & Command Handler
        API[Unified REST API<br/>Create Charge & Sibling VAs]
        CALLBACK[Bank Callback Controller<br/>Inquiry & Payment Hooks]
        VALIDATOR[Command Validator & State Machine<br/>Idempotency & Invariant Rules]
    end

    subgraph Event Store
        KAFKA[ Apache Kafka Cluster <br/>Immutable Source of Truth Event Log]
    end

    subgraph Kafka Streams & Hot-Path Engine
        subgraph App_Node ["App Instance (JVM Process)"]
            KSTREAM_ENGINE[Kafka Streams Processor Topology]
            subgraph RocksDB_Stores ["Embedded RocksDB State Stores (Off-Heap / Disk)"]
                RDB_CHARGE[(charge-state-store)]
                RDB_VA[(va-registry-store)]
                RDB_IDEM[(idempotency-store)]
            end
        end
    end

    subgraph CQRS Projection Sink & Workers
        PROJ_SINK[PostgreSQL Projection Sink<br/>Kafka Event Consumer Group]
        WH_WORKER[Webhook Dispatcher Worker<br/>Asynchronous Event Delivery]
        RECON_ENGINE[Reconciliation Engine<br/>CSV Importer & Discrepancy Checker]
    end

    subgraph Read Models & Persistence
        PG[(PostgreSQL 18<br/>Reporting Read DB)]
        ADMIN[Web Admin UI & Reporting<br/>Thymeleaf / HTMX + Spring Data JPA]
    end

    %% Relationships
    CLIENT -->|1. Create Charge Request| API
    B1 -->|2. Inquiry / Payment Callback| CALLBACK
    B2 -->|2. Inquiry / Payment Callback| CALLBACK
    B3 -->|2. Inquiry / Payment Callback| CALLBACK

    API -->|3. Submit Command| VALIDATOR
    CALLBACK -->|3. Submit Callback Command| VALIDATOR

    VALIDATOR -->|4. Append Domain Events| KAFKA

    KAFKA -->|5. Stream Domain Events| KSTREAM_ENGINE
    KSTREAM_ENGINE -->|6. Maintain Local Key-Value State| RocksDB_Stores

    KAFKA -->|7. Stream Events to Projection Sink| PROJ_SINK
    PROJ_SINK -->|8. Idempotent Upsert| PG

    KAFKA -->|Stream Payment Events| WH_WORKER
    WH_WORKER -->|9. Deliver Webhook| CLIENT

    RECON_ENGINE -->|Upload EOD CSV & Match| PG
    RECON_ENGINE -->|Emit Recon Events| KAFKA

    ADMIN -->|10. Spring Data JPA Queries| PG
```

#### 2. Traditional RDBMS System Architecture (Original `payment-gateway`)

Traditional monolithic architecture where all core modules, callback controllers, webhooks, and admin UI query and write directly to a single shared PostgreSQL database using synchronous ACID transactions.

```mermaid
flowchart TD
    subgraph Relational Gateway Service ["payment-gateway (Monolithic RDBMS App)"]
        API[Unified REST API<br/>Create Charge & Sibling VAs]
        CALLBACK[Bank Callback Controller<br/>Inquiry & Payment Hooks]
        CORE[VA Registry & Lifecycle Engine<br/>Open / Closed / Installment]
        NOTIF[Webhook Notification Engine]
        RECON[Reconciliation Engine<br/>CSV Importer]
        ADMIN[Web Admin UI & Reporting<br/>Thymeleaf / HTMX]
    end

    subgraph Relational Persistence
        PG[(PostgreSQL 18 Database<br/>Synchronous CRUD & ACID Transactions)]
    end

    %% Relationships
    CLIENT -->|1. Create Charge| API
    B1 -->|2. Inquiry / Payment Callback| CALLBACK
    B2 -->|2. Inquiry / Payment Callback| CALLBACK
    B3 -->|2. Inquiry / Payment Callback| CALLBACK

    API --> CORE
    CALLBACK --> CORE
    CORE -->|Synchronous SQL Transactions| PG
    NOTIF -->|Read & Deliver Webhooks| PG
    RECON -->|EOD CSV Sync Writes| PG
    ADMIN -->|SQL Queries| PG

    NOTIF -->|Deliver Webhook| CLIENT
```

---

### 3.5 Synchronous HTTP Protocol Adapters & Hot-Path Pre-Validation

Indonesian banking protocols (Maybank SNAP, BSI, CIMB) mandate **strict synchronous HTTP responses**. The gateway must never return an intermediate "pending" state to the bank. This section describes the pre-validation actually implemented in `PaymentApplicationService.processPayment` / `InquiryApplicationService.inquireAccount`, executed on the HTTP request thread as interactive queries against the Kafka Streams RocksDB state stores — not a design aspiration.

1. **Account Inquiry** (`POST /api/v1/inquiry`, `/api/inquiry`, `/api/bank/maybank/v1.0/transfer-va/inquiry`): responds **`HTTP 200 OK`** with customer name and outstanding amount on a resolved VA, or **`HTTP 404`** (`INVALID_VA` / `INVALID_CHARGE`) on an unresolved one.
2. **Payment Callback** (`POST /api/v1/payments`, `/api/payments`, `/api/bank/maybank/v1.0/transfer-va/payment`; BSI's proprietary shape is served separately at `/api/bank/bsi`, see below): the outcome vocabulary is the `PaymentOutcome` enum (`ACCEPTED`, `DUPLICATE`, `REJECTED_INVALID_VA`, `REJECTED_CHARGE_CLOSED`, `REJECTED_INVALID_AMOUNT`, `REJECTED_INVALID_REQUEST`), mapped to HTTP status by `BankCallbackController`:
   - `ACCEPTED`, `DUPLICATE` → `200 OK`
   - `REJECTED_INVALID_VA` → `404 Not Found`
   - `REJECTED_CHARGE_CLOSED`, `REJECTED_INVALID_AMOUNT`, `REJECTED_INVALID_REQUEST` → `400 Bad Request`

```mermaid
flowchart TD
    subgraph Bank_Call ["Bank Protocols (Maybank SNAP / BSI / CIMB)"]
        INQ_REQ["1. Account Inquiry Request<br/>(POST /api/v1/inquiry)"]
        PAY_REQ["2. Payment Callback Request<br/>(POST /api/v1/payments)"]
    end

    subgraph App_Instance ["Application Server (JVM Process)"]
        CTRL[Protocol Ingress Controller]
        subgraph Local_RocksDB ["Local RocksDB, interactive query on the request thread"]
            VA_STORE[("va-registry-store")]
            CHG_STORE[("charge-state-store")]
            IDEM_STORE[("idempotency-store")]
        end
        PROD[Kafka Producer]
    end

    subgraph Async_Engine ["Asynchronous Event Core & Sinks"]
        KAFKA[(Kafka Topic: payment-events)]
        WEBHOOK[Webhook Dispatcher]
        PG[(PostgreSQL 18 Read DB)]
    end

    %% INQUIRY FLOW
    INQ_REQ -->|"a. Sync HTTP POST"| CTRL
    CTRL -->|"b. Lookup VA & charge"| Local_RocksDB
    CTRL -->|"c1. Resolved -> HTTP 200 OK"| INQ_REQ
    CTRL -->|"c2. Not found -> HTTP 404 INVALID_VA"| INQ_REQ

    %% PAYMENT FLOW
    PAY_REQ -->|"a. Sync HTTP POST"| CTRL
    CTRL -->|"b. Idempotency, VA, charge-status checks, in order"| Local_RocksDB

    CTRL -->|"c1. Malformed request -> HTTP 400 REJECTED_INVALID_REQUEST"| PAY_REQ
    CTRL -->|"c2. Duplicate bankReference -> HTTP 200 DUPLICATE"| PAY_REQ
    CTRL -->|"c3. Unknown VA -> HTTP 404 REJECTED_INVALID_VA"| PAY_REQ
    CTRL -->|"c4. Charge already PAID -> HTTP 400 REJECTED_CHARGE_CLOSED"| PAY_REQ
    CTRL -->|"c5. Passed all checks -> Append PaymentReceivedEvent, block for the send ack"| PROD
    PROD --> KAFKA
    CTRL -->|"c6. HTTP 200 ACCEPTED"| PAY_REQ

    %% ASYNC FANOUT
    KAFKA -.-> WEBHOOK
    KAFKA -.-> PG
```

#### Detailed Execution Mechanics:

1. **Account Inquiry**: the controller looks up `bankCode_vaNumber` in `va-registry-store`, then the resolved `chargeId` in `charge-state-store`, and returns `200 OK` with the current outstanding amount, or `404` if either lookup misses.

2. **Payment Callback Pre-Validation** (`PaymentApplicationService.processPayment`, in this exact order — each step short-circuits the rest):
   1. **Field validation**: `bankCode`, `vaNumber`, `bankReference` non-blank, `amount` present and `> 0`, `paymentTimestamp` present. Any violation → `REJECTED_INVALID_REQUEST` (`400`). No field is defaulted or substituted — a missing `paymentTimestamp` is rejected, never set to `now()`.
   2. **Idempotency**: look up `bankCode + "_" + bankReference` in `idempotency-store`. A hit returns `DUPLICATE` (`200`) with the originally recorded `eventId`/`chargeId` — no second event is appended.
   3. **VA resolution**: look up `bankCode + "_" + vaNumber` in `va-registry-store`. A miss returns `REJECTED_INVALID_VA` (`404`). The caller does not supply `chargeId` — the gateway resolves it from the VA.
   4. **Charge terminal-status check**: load the resolved charge from `charge-state-store`. If its status is `PAID`, the payment is rejected as `REJECTED_CHARGE_CLOSED` (`400`) **and** a `DoubleSettlementDetectedEvent` is appended to `payment-events` immediately, flagging the attempted overpayment rather than silently dropping it.
   5. Otherwise, a `PaymentReceivedEvent` is appended to `payment-events` via `kafkaTemplate.send(...).get()` (the request blocks for the broker ack) and `ACCEPTED` (`200`) is returned.

   **Not implemented**: `REJECTED_INVALID_AMOUNT` is declared in the outcome enum and already wired to `400` in both callback controllers, but no code path currently produces it. Per-charge-type amount validation (CLOSED payment must equal the remaining balance; INSTALLMENT must not exceed it) described as a target in earlier design notes does not exist yet — the only amount check on the request thread is the `> 0` field check above.

3. **Residual race, and where it's actually closed**: this pre-validation runs on the request thread and is **not** the authoritative serialization point — two concurrent callbacks against the same charge can both pass step 4 before either `PaymentReceivedEvent` is applied. `PaymentEventProcessor` in `PaymentGatewayStreamsTopology` (the single writer per `chargeId` partition key) re-checks idempotency and the charge's terminal status before applying `cumulativePaid`, and emits its own `DoubleSettlementDetectedEvent` instead of applying an overpayment if that race actually occurred. A concurrent-settlement test (`BankCallbackControllerIntegrationTest.testPaymentCallback_ConcurrentFullSettlement_ExactlyOneApplied`) exercises this end to end.

4. **Asynchronous fan-out**: after the bank receives its synchronous response, `WebhookDispatcherWorker` and `PostgresProjectionSink` consume `payment-events` (and the other domain topics) independently to deliver client webhooks and update the PostgreSQL read model. Neither is on the request path.

#### 3.5.1 Internal Uniform Correlation ID vs. External Bank Correlation ID Mapping

To maintain strict architectural consistency across heterogeneous bank protocols while preserving full auditability, `payment-gateway-evtsrc` separates correlation identifiers into two distinct tiers:

1. **Internal Uniform Correlation ID (`correlationId` / `eventId`)**:
   - **Format**: Standardized internal `UUID` (e.g. `UUID.randomUUID()`) generated by the gateway.
   - **Purpose**: Guarantees a consistent, time-ordered primary key across all internal application logs, Kafka event keys, RocksDB state stores, and PostgreSQL CQRS projection tables, regardless of which bank sent the callback.
2. **External Bank Correlation ID (`externalCorrelationId` / `bankReference`)**:
   - **Format**: Raw string provided by the bank or protocol adapter (e.g. SNAP `X-EXTERNAL-ID`, REST `X-Correlation-ID`, or `bankReference`). Formats vary widely across banks (alphanumeric, variable length, or missing).
   - **Purpose**: Maps internal events back to the bank's external reference for audit inquiries, EOD CSV settlement matching, and outbound client webhook headers (`X-Correlation-ID`).

#### Propagation Lifecycle:
- **Ingress Extraction**: Controller receives callback, generates internal `eventId` (UUID), and extracts `externalCorrelationId` from request headers/payload.
- **Kafka Event Enrichment**: `PaymentReceivedEvent` contains both `eventId` (internal UUID) and `externalCorrelationId` (bank reference).
- **RocksDB Idempotency**: `idempotency-store` indexes transactions by `externalCorrelationId` / `bankReference` to block duplicate bank callbacks within $<1\text{ms}$.
- **PostgreSQL CQRS Projection**: Read models store both `event_id` (UUID primary key) and `external_correlation_id`, allowing operators to search dashboard logs by either internal UUID or bank reference.
- **Outbound Webhook Delivery**: `WebhookDispatcherWorker` attaches `X-Correlation-ID: <externalCorrelationId>` when delivering HTTP POST notifications to merchant subledgers (e.g. `account-receivable`).

#### 3.5.2 Monolithic In-Process State vs. Distributed Microservices Correlation

A key architectural design question when building payment gateways is whether to use **In-Process Synchronous Validation** or **Deferred Synchronous Correlation**:

1. **Single-Module Monolithic Layout with Local RocksDB (`payment-gateway-evtsrc`)**:
   - **Mechanism**: The ingress controller, state stores, and stream topologies live in the same Spring Boot application process.
   - **Validation**: When a bank callback (`POST /api/v1/payments`) arrives, the HTTP request thread queries `idempotency-store`, `va-registry-store`, and `charge-state-store` directly in local RocksDB RAM/SSD ($<1\text{ms}$).
   - **Result**: The controller accepts or rejects the callback **in-process before returning**. It appends the event to Kafka and returns `HTTP 200 OK` directly on the request thread. **No `CompletableFuture` or broadcast consumer groups are required.**

2. **Distributed Microservices Layout (e.g. Ingress Gateway + Independent Bank Host Adapters / Clearing Core Microservices)**:
   - **Mechanism**: The Ingress Gateway is decoupled into a thin edge microservice that does *not* host state, while downstream business logic (e.g. fraud screening, core settlement engine, or dedicated per-bank host adapter microservices) runs in separate application containers.
   - **Validation**: When a request arrives, the Ingress Gateway microservice cannot validate state locally. It must publish a command event to Kafka (e.g. `bank-request-topic`) and **defer the HTTP response**.
   - **Result**: The HTTP request thread registers a `CompletableFuture<Response>` keyed by correlation ID (`correlationId` / `bankReference`) and blocks on `future.get(timeout)`. Each Ingress Gateway replica runs a broadcast consumer group (`ingress-gateway-${instance-id}`) listening on `bank-response-topic` to correlate the outcome back to the waiting HTTP thread.

#### Architectural Trade-off Comparison:

| Metric / Aspect | Single-Module Monolith (`payment-gateway-evtsrc`) | Distributed Microservices Layout |
|---|---|---|
| **State Location** | Embedded off-heap **RocksDB** on local App node. | External microservices / database stores across network. |
| **Validation Point** | **In-Process** (HTTP thread queries RocksDB directly). | **Out-of-Process** (Ingress waits for downstream Kafka event). |
| **Sync Response Latency** | **$<1\text{ ms}$** | **$50\text{--}500\text{ ms}$** |
| **Correlation Strategy** | Standard event logging (`correlationId` header). | **Deferred `CompletableFuture` + Broadcast Consumer Groups**. |
| **Operational Complexity** | **Low** (Single deployment artifact, zero fanout network traffic). | **High** (Per-instance consumer groups, network traffic amplification). |

### 3.6 Initial Deployment Seeding for Pre-Existing Virtual Accounts & Charges

When deploying `payment-gateway-evtsrc` into an existing enterprise environment with pre-existing charges and bank VAs, initial state can be loaded directly from a **PostgreSQL database dump**, via `PostgresInitialStateSeeder`:

1. **Database Dump Import**: DBAs restore legacy tables directly into PostgreSQL tables (`charge_projection`, `sibling_va_projection`).
2. **On-Startup Registration**: When `app.migration.seed-from-postgres=true` is set and the settlement store is empty, `PostgresInitialStateSeeder` runs on application startup:
   - Reads active pre-existing charges and Virtual Accounts from PostgreSQL.
   - Registers each one directly into `ChargeSettlementStore` (the same RocksDB `TransactionDB` the request-thread write path owns), synchronously, so they are resolvable via inquiry/payment the instant seeding finishes.
   - Also emits the equivalent `ChargeCreatedEvent`/`SiblingVaRegisteredEvent` records to `charge-events`/`va-events`, purely so `PostgresProjectionSink` builds the matching read model.
3. **Idempotent re-run guard, not changelog-backed durability**: `ChargeSettlementStore` is a plain, directly-owned RocksDB directory with no Kafka Streams changelog behind it. If `app.settlement-store.dir` is a persistent volume, the directory survives a restart and the seeder detects existing state and skips re-seeding. If it isn't, the store comes back empty and the seeder runs again on the next restart -- give it a persistent volume for a real migration.

---

## 4. Production Sizing, High Availability & Operational Scalability

### 4.1 Comparative Production Expectations & Scaling Mechanics

| Operational Dimension | Traditional RDBMS (`payment-gateway`) | Event-Sourced CQRS (`payment-gateway-evtsrc`) |
|---|---|---|
| **Scaling Mechanism** | **Vertical Scaling (Scale-Up)**: Increase CPU, RAM, and NVMe IOPS on Primary PostgreSQL DB. Read Replicas offload read queries only. | **Horizontal Scaling (Scale-Out)**: Distribute topic partitions across additional Kafka Streams application instances and Kafka brokers. |
| **Write Throughput Bottleneck** | **Single Primary DB Writer**: All bank callbacks hit the single primary database instance for `SELECT FOR UPDATE` and `COMMIT`. | **Kafka Partition Count**: Writes are partitioned across Kafka topics and processed in parallel across app nodes. |
| **Failover Recovery SLA** | **10–30 seconds** (DB failover via Patroni/PgBouncer with connection re-establishment). | **<1 second** (Warm RocksDB standby replica promotion on peer app instance). |
| **Operational Complexity** | **Low**: Standard Spring Boot CRUD app + single PostgreSQL DB cluster. Easy debugging and deployment. | **Moderate to High**: Requires managing Kafka cluster, Kafka Streams topologies, RocksDB off-heap memory, and async projection sinks. |

---

### 4.2 Scalability Limitations & Bottlenecks

#### 1. Traditional RDBMS Approach Limitations (`payment-gateway`)
- **Primary Database Write Bottleneck**: While Read Replicas offload read traffic, all bank callback writes (`INSERT INTO payment`, `UPDATE charge SET balance = ...`) must execute on the single Primary PostgreSQL writer node inside a synchronous ACID transaction. Under heavy enrollment-scale bursts (e.g. tuition payment deadline), row-level locks (`SELECT FOR UPDATE`) cause thread pool exhaustion and bank callback timeouts (`504 Gateway Timeout`).
- **I/O Contention During Reconciliation**: End-of-Day (EOD) CSV reconciliation imports execute heavy batch writes and table scans on the primary database, consuming disk IOPS and CPU, directly degrading callback SLA for real-time payments.
- **Connection Pool Exhaustion**: High concurrent HTTP callback requests rapidly consume available PgBouncer / HikariCP connection pools, risking connection rejection under traffic spikes.
- **Destructive State Updates**: `UPDATE` queries overwrite past state, destroying historical timeline auditability unless complex audit tables and database triggers are maintained.

#### 2. Event-Sourced CQRS Approach Limitations (`payment-gateway-evtsrc`)
- **Partition Count Bounded Parallelism**: Processing parallelism in Kafka Streams is strictly bounded by the number of partitions per Kafka topic. Increasing parallelism beyond the initial partition count requires a topic re-partitioning migration and state re-hydration.
- **Storage Footprint Amplification**: Events are stored across three storage tiers: (1) immutable Kafka topic segment files, (2) embedded local RocksDB SSTable files on app instances, and (3) relational projection tables in PostgreSQL 18.
- **Eventual Consistency & Projection Lag**: there is an inherent lag between Kafka event emission and PostgreSQL table update. `PostgresProjectionSink` exposes it live at `GET /api/admin/debug/projection-lag` (`{"lagMillis": null}` until the first payment is projected); no lag figure has been measured under load with the current batch-listener sink, so no number is quoted here — read the endpoint during any real benchmark run instead of assuming a value.
- **Off-Heap C++ Native Memory Management**: RocksDB operates outside the JVM heap. Improper memory configuration (block cache, memtable bounds) can cause Linux OS OOM-killer to terminate application containers unexpectedly under heavy write pressure.

---

### 4.3 Initial Partition Count Decision Framework & Sizing Guide

To guarantee strict event ordering and local state joins without inter-node network hops, all Kafka topics use a co-partitioned key strategy (`charge_id`).

#### 1. The Engineering Sizing Formula

In Apache Kafka and Kafka Streams, partition count determines the **maximum horizontal parallelism** of application worker threads:

$$\text{Partitions} = \max\left( \frac{\text{Target Write Throughput (MB/s)}}{\text{Single Producer Max Throughput}}, \frac{\text{Target Read Throughput (MB/s)}}{\text{Single Consumer Max Throughput}} \right)$$

Key constraints:
- You **can never run more active application stream threads than there are topic partitions**. (e.g. 12 partitions allows scaling up to 12 threads across 1, 2, 3, 4, 6, or 12 container instances).
- Each partition maps to an independent **local RocksDB state store partition** (`/var/data/rocksdb/partition_N`).

#### 2. Metrics Required to Determine Initial Partition Count
1. **Target Peak Throughput (TPS)**: Maximum expected bank callback TPS during enrollment/tuition deadline spikes.
2. **Planned App Container Nodes**: Number of container instances and stream threads per node (e.g. 3 nodes $\times$ 4 stream threads = 12 total worker threads).
3. **Divisibility Factor**: Selecting a partition count highly divisible by common node counts ($1, 2, 3, 4, 6, 12$) enables even workload distribution during horizontal scale-out.

#### 3. Recommended Partition Sizing Matrix

| Scale Tier | Target Peak Throughput | App Nodes & Threads | Recommended Partitions | Scaling & Infrastructure Notes |
|---|---|---|---|---|
| **Starter / Development** | $<500\text{ TPS}$ | 1 App Instance (6 threads) | **6 Partitions** | **1:1 Infrastructure Parity** with single-node RDBMS baseline. |
| **Production Baseline** | $1,000\text{--}3,000\text{ TPS}$ | 3 App Instances (4 threads/node = 12 threads) | **12 Partitions** | Divisible by 1, 2, 3, 4, 6, 12 instances; supports seamless scale-out. |
| **High-Scale Enterprise** | $5,000\text{--}10,000+\text{ TPS}$ | 6 App Instances (4 threads/node = 24 threads) | **24 Partitions** | Maximum parallel processing across multi-AZ container clusters. |

> **Why 12 Partitions is the Default Production Baseline**: In Kafka Streams, increasing topic partitions after initial deployment requires topic re-partitioning and re-hydrating RocksDB state stores. Selecting **12 partitions** upfront allows scaling from 1 to 12 instances smoothly without re-partitioning topics.

#### 4. Current Implementation Status

Topics are created explicitly by `KafkaTopicConfig` (`NewTopic` beans for `charge-events`, `va-events`, `payment-events`, `reconciliation-events`, `webhook-events`), not left to Kafka's auto-creation default of 1 partition. Partition count is the single property `app.kafka.partitions` (default **6**, matching the Starter tier above), `replicationFactor` **1**. `spring.kafka.listener.concurrency` for the projection sink's batch listener is driven by the same property. `spring.kafka.streams.num.stream.threads` is still hardcoded at **6** independently of `app.kafka.partitions` — raising the partition count without also raising this value under-utilizes the extra partitions. No throughput numbers below reflect a run against this partitioning; they are unmeasured until a benchmark is executed with it in place.

---

### 4.4 Production High-Availability & Replication Topologies

#### 1. Event-Sourced CQRS HA Topology (`payment-gateway-evtsrc`)

For high-availability production deployment, the event-sourced architecture implements a multi-tier quorum and warm standby strategy:

```mermaid
flowchart TD
    subgraph Edge ["Traffic Routing"]
        LB[Load Balancer / Ingress Router]
    end

    subgraph App_Tier ["Kafka Streams Processing Tier (3 App Instances)"]
        subgraph APP1 ["App Instance 1"]
            A1_ACT["Active Partitions 1..4"]
            A1_STB["Standby Partitions 5..8 (RocksDB)"]
        end
        subgraph APP2 ["App Instance 2"]
            A2_ACT["Active Partitions 5..8"]
            A2_STB["Standby Partitions 9..12 (RocksDB)"]
        end
        subgraph APP3 ["App Instance 3"]
            A3_ACT["Active Partitions 9..12"]
            A3_STB["Standby Partitions 1..4 (RocksDB)"]
        end
    end

    subgraph Kafka_Tier ["Kafka Cluster Quorum (Storage Layer)"]
        K1[Broker 1]
        K2[Broker 2]
        K3[Broker 3]
        KAFKA_CFG["RF = 3 | min.isr = 2 | acks = all"]
    end

    subgraph DB_Tier ["PostgreSQL 18 HA Cluster (Reporting Layer)"]
        PG_PRI[(PostgreSQL Primary)]
        PG_REP[(PostgreSQL Streaming Replica)]
        PATRONI["Patroni + PgBouncer HA Pooler"]
    end

    %% Ingress routes
    LB --> APP1
    LB --> APP2
    LB --> APP3

    %% App to Kafka
    APP1 <-->|Read/Write Streams & Changelogs| Kafka_Tier
    APP2 <-->|Read/Write Streams & Changelogs| Kafka_Tier
    APP3 <-->|Read/Write Streams & Changelogs| Kafka_Tier

    %% Kafka to Postgres
    Kafka_Tier -->|Async CQRS Event Stream| PG_PRI
    PG_PRI -->|Streaming Replication| PG_REP
    PATRONI --- PG_PRI
    PATRONI --- PG_REP
```

##### Event-Sourced HA Mechanics:
- **Kafka Storage Quorum**: Minimum **3 Kafka Brokers** (KRaft mode). `replication.factor = 3`, `min.insync.replicas = 2`, `acks = all`. Tolerates failure of 1 broker without data loss ($N=3, W=2, R=2$).
- **Kafka Streams Warm Standbys**: App instances run with `num.standby.replicas = 1`. Peer app nodes maintain shadow RocksDB standby replicas in background by consuming changelog topics. If **App Instance 1** fails, **App Instance 2** promotes its warm local RocksDB standby to active in **$<1\text{ second}$**, eliminating network state re-hydration.
- **Non-Blocking Reporting HA**: PostgreSQL 18 is managed by Patroni + PgBouncer. If PostgreSQL fails completely, bank callbacks continue operating without interruption; events buffer in Kafka until Postgres recovers.

---

#### 2. Traditional RDBMS HA Topology (Original `payment-gateway`)

For high-availability in the traditional relational deployment, all app instances connect to a primary database cluster managed by connection poolers:

```mermaid
flowchart TD
    subgraph Edge ["Traffic Routing"]
        LB[Load Balancer / Ingress Router]
    end

    subgraph App_Tier ["Monolithic App Tier (3 App Instances)"]
        APP1[App Instance 1]
        APP2[App Instance 2]
        APP3[App Instance 3]
    end

    subgraph Pooler_Tier ["Connection Pooling"]
        PGB[PgBouncer Connection Pooler]
    end

    subgraph RDBMS_HA ["PostgreSQL 18 Primary-Replica Cluster"]
        PG_PRI[(PostgreSQL 18 Primary <br/> All Writes & Reads)]
        PG_REP[(PostgreSQL 18 Read Replica <br/> Reporting Only)]
        PATRONI["Patroni + Etcd DCS <br/> Auto-Failover Orchestrator"]
    end

    %% Routing
    LB --> APP1
    LB --> APP2
    LB --> APP3

    APP1 --> PGB
    APP2 --> PGB
    APP3 --> PGB

    PGB -->|All Write Transactions & Callbacks| PG_PRI
    PGB -.->|Read-Only Admin Queries| PG_REP

    PG_PRI -->|Streaming Replication| PG_REP
    PATRONI --- PG_PRI
    PATRONI --- PG_REP
```

##### RDBMS HA Mechanics:
- **Single Primary Writer**: All 3 application instances write to the single Primary PostgreSQL 18 database through PgBouncer.
- **Failover SLA (RTO 10–30s)**: If the Primary DB node fails, Patroni promotes the Read Replica to Primary and re-routes PgBouncer connections. During this $10\text{--}30\text{ second}$ failover window, **all incoming bank callbacks fail or time out**.
- **Vertical Bottleneck**: Increasing bank callback throughput requires scaling up the Primary DB machine's CPU, RAM, and NVMe IOPS. Adding app instances does not increase write capacity.

---

### 4.5 Event Stream Replay & Projection Rebuild Runbook

One of the key benefits of this Event Sourcing setup is the ability to wipe the PostgreSQL reporting database and rebuild all read models from genesis:

1. **Stop Projection Sink Consumer**: Pause the `projection-sink` consumer group.
2. **Apply Flyway Schema Migration**: Truncate or drop/recreate PostgreSQL projection tables (`charge_projection`, `payment_projection`, `reconciliation_projection`).
3. **Reset Consumer Group Offset**:
   ```bash
   kafka-consumer-groups --bootstrap-server kafka:9092 \
     --group payment-gateway-projection-sink \
     --reset-offsets --to-earliest --execute --topic charge-events,payment-events,va-events
   ```
4. **Restart Projection Sink**: The consumer group replays all events from `offset 0` into PostgreSQL. `projection_lag_ms` drops back to `0` upon completion.

---

## 5. Performance Benchmark & Comparison

Both repositories are benchmarked through the identical real BSI proprietary adapter workload
(`POST /api/bank/bsi`, full SHA-1 checksum, `scenarios/suite-bsi.js` here / `scenarios/suite-rdbms.js`
on the RDBMS side), a `ramping-arrival-rate` profile from 50 to 2,000 TPS over 90 seconds, on the
same 6 BSI VA/amount pairs from `scenarios/seed-data.json`. Both scripts require `RUN_ID` and
`BSI_SHARED_SECRET` as environment variables with no default (a missing value throws in the k6 init
stage — the exact "empty/`\"null\"` secret" bug this replaces is documented in
`docs/benchmark-remediation-guideline.md` finding F5).

**Full methodology, both runs' numbers, the financial-correctness audit, and a real finding about
performance degrading under sustained load on a small hot-row dataset are in
[`scenarios/perf_benchmark_report.md`](scenarios/perf_benchmark_report.md).** Headline result: both
systems pass their error-rate and p99 thresholds; RDBMS is faster at the median (a single SQL
transaction vs. three RocksDB queries plus a Kafka round-trip), evtsrc holds up better under
sustained contention on the same small set of rows.

Reproduce with `./scenarios/run-benchmark.sh <target-url>` (evtsrc) or the equivalent direct `k6 run`
invocation documented in `suite-rdbms.js`'s header (RDBMS), then audit with
`scenarios/verify-correctness.py --k6-results ... --run-id ...` (pass `--target evtsrc` or
`--target rdbms` if both systems' database containers are running at once — auto-detection refuses
to guess in that case).

---

## 6. Implementation Blueprint & Stack

### 6.1 Project Module Layout & Application Structure

For maximum **1:1 repository parity** with the relational `payment-gateway` baseline, `payment-gateway-evtsrc` is structured as a **single Spring Boot application module** (single `pom.xml`). All CQRS write paths, Kafka Streams topologies, projection sinks, and web controllers live within a single application artifact, organized by package boundary:

#### Single-Module Layout (Recommended Default for 1:1 Parity)
```
payment-gateway-evtsrc/
├── src/
│   ├── main/
│   │   ├── java/com/artivisi/paymentgateway/
│   │   │   ├── domain/             # Immutable Event DTOs & Aggregate State Models
│   │   │   ├── web/
│   │   │   │   ├── api/            # Bank Callback Controllers (Maybank SNAP, BSI REST, CIMB SOAP)
│   │   │   │   └── admin/          # Operator Dashboard Controllers (Thymeleaf / HTMX)
│   │   │   ├── streams/            # Kafka Streams Topologies & Embedded RocksDB Stores
│   │   │   ├── projection/         # PostgreSQL 18 Projection Sink & Spring Data JPA Repositories
│   │   │   └── config/             # Kafka, RocksDB, and Security Configuration
│   │   └── resources/
│   │       ├── db/migration/       # Flyway SQL Migration Scripts (PostgreSQL 18 Schema)
│   │       ├── templates/          # Thymeleaf UI Templates
│   │       └── application.yml
│   └── test/                       # Testcontainers (Kafka & Postgres) Integration Tests
├── compose.yml                     # Local Dev Infrastructure (Kafka KRaft, PostgreSQL 18)
└── pom.xml                         # Single Maven Build Descriptor
```

#### Alternative Multi-Module Layout (Optional for Microservice Isolation)
If enterprise deployment teams require running `ingress-gateway` and `streams-engine` as independently deployed container artifacts, the package hierarchy can optionally be split into Maven submodules (`message-model`, `ingress-gateway`, `streams-engine`, `projection-sink`, `admin-web`).

---

### 6.2 Tech Stack

| Layer | Technology |
|---|---|
| **Language & Runtime** | Java 25 |
| **Framework** | Spring Boot 4.1.0 (Spring Web, Spring Data JPA, Spring Security) |
| **Event Store & Streaming** | Apache Kafka & Kafka Streams |
| **Hot-Path State Store** | RocksDB (`rocksdbjni`) |
| **Reporting Projection DB** | PostgreSQL 18 + Flyway Migrations |
| **Integration & Test Suite** | Testcontainers (Kafka & PostgreSQL 18), JUnit 5, RestAssured |
| **Performance Benchmarking** | **k6** (Reusable load testing scripts across RDBMS & CQRS) |
| **Local Infrastructure** | Docker Compose (`compose.yml`) |
| **Repository** | Git (`artivisi` namespace) |
| **License** | Apache License 2.0 |

---

## 7. Public Repository Information

- **GitHub Repository**: [`git@github.com:artivisi/payment-gateway-evtsrc.git`](https://github.com/artivisi/payment-gateway-evtsrc)
- **Namespace**: `artivisi`
- **License**: Apache License 2.0 — see [`LICENSE`](LICENSE) for full details.
