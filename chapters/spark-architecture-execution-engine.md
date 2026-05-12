
# 1.1 Spark Architecture Overview (Staff-Level Master Notes)

---

## 1. What is Spark?

Apache Spark is a distributed execution engine designed to process large-scale datasets across a cluster of machines in parallel.

At a deeper level, Spark is:

> A distributed execution system that converts user-defined transformations into optimized execution plans (DAGs) and executes them across a cluster using fault-tolerant, partition-based parallelism.

Spark does not execute immediately. It builds a full execution plan before running anything.

---

## 2. What Problem Does Spark Solve?

### 2.1 Disk-based computation bottleneck
Earlier systems wrote intermediate results to disk between stages, causing:
- High I/O overhead
- Slow execution
- Poor iterative processing performance

---

### 2.2 Lack of global optimization
Traditional systems executed each step independently, preventing:
- Query-wide optimization
- Join optimization across stages
- Filter pushdown across pipelines

---

### 2.3 Inefficient iterative workloads
Machine learning and graph processing require repeated computation on the same dataset. Older systems were inefficient due to disk-based pipelines.

---

### 2.4 Rigid execution model
Fixed execution pipelines prevented dynamic optimization and flexible scheduling.

---

## 3. How Spark Solves These Problems

### 3.1 In-memory computation
Spark keeps intermediate data in memory where possible, reducing disk I/O.

---

### 3.2 DAG execution model
Instead of fixed pipelines, Spark builds a Directed Acyclic Graph (DAG) representing full execution flow.

---

### 3.3 Lazy evaluation
Transformations are not executed immediately. Execution happens only when an action is triggered.

---

### 3.4 Lineage-based fault tolerance
Instead of storing intermediate results, Spark records how data is generated and recomputes it when needed.

---

## 4. Spark Architecture Components

### 4.1 Driver (Control Plane)
The Driver is responsible for:
- Building execution plans (DAG)
- Scheduling tasks
- Tracking execution state
- Coordinating executors

Internally includes:
- DAG Scheduler
- Task Scheduler
- SparkContext
- MapOutputTracker
- BlockManagerMaster

---

### 4.2 Executors (Compute Plane)
Executors are distributed JVM processes that:
- Execute tasks
- Store shuffle data
- Cache intermediate results
- Report execution metrics

Each executor includes:
- Thread pool
- Memory manager
- Shuffle manager
- Block manager

---

### 4.3 Cluster Manager (Resource Layer)
Examples:
- YARN
- Kubernetes
- Standalone Spark
- Databricks cluster manager

Responsibilities:
- Allocate CPU and memory
- Launch executors
- Manage cluster resources

---

### 4.4 Storage Layer
Spark reads/writes data from:
- HDFS
- Amazon S3
- Azure Data Lake Storage
- Google Cloud Storage
- Delta Lake

---

### 4.5 Shuffle Layer
Shuffle handles:
- Data redistribution between executors
- Intermediate disk writes
- Network transfer of partitions

It is the most expensive operation in Spark execution.

---

## 5. Execution Flow (What Happens When a Job Runs)

### Step 1: Code submission
User submits transformations and actions.

---

### Step 2: Logical plan creation
Spark builds a logical representation of operations.

---

### Step 3: Catalyst optimization
Spark optimizes the logical plan:
- Predicate pushdown
- Column pruning
- Expression simplification

---

### Step 4: Physical plan creation
Spark decides execution strategy:
- Join types
- Aggregation strategy
- Partitioning strategy

---

### Step 5: DAG generation
Execution is split into stages:
- Narrow transformations → same stage
- Wide transformations → new stage (shuffle boundary)

---

### Step 6: Task creation
Each partition becomes a task.

---

### Step 7: Resource allocation
Driver requests executors from cluster manager.

---

### Step 8: Task execution
Executors execute tasks in parallel.

---

### Step 9: Shuffle (if required)
Data is redistributed across executors.

---

### Step 10: Result returned
Final output is returned or stored.

---

## 6. Failure Model

### 6.1 Driver Failure
If the Driver fails:
- DAG is lost
- Scheduler state is lost
- Executor coordination stops
- Entire job fails

Spark does not persist execution state externally.

---

### 6.2 Executor Failure
If an executor fails:
- Running tasks are lost
- Driver reschedules tasks
- Lost data is recomputed using lineage

If shuffle data is lost:
- Upstream stages are recomputed

---

### 6.3 Shuffle Failure
If shuffle data is lost:
- Dependent stages are recomputed
- Execution time increases significantly

---

## 7. Why Spark is Different from Traditional Systems

### 7.1 Execution model
Spark uses DAG-based execution instead of fixed pipelines.

---

### 7.2 Optimization
Spark uses global query optimization (Catalyst).

---

### 7.3 Fault tolerance
Spark uses lineage-based recomputation instead of replication.

---

### 7.4 Flexibility
Spark supports:
- Batch processing
- Streaming
- Machine learning
- Graph processing

---

## 8. Key Production Issues

- Shuffle bottlenecks dominate runtime
- Data skew causes stage delays
- Executor memory spill impacts performance
- Driver becomes bottleneck at scale
- Cloud storage increases latency

---

## 9. Common Anti-Patterns

- Using collect() on large datasets
- Too many small partitions
- Excessive caching
- Wide transformations without optimization
- Ignoring data skew

---

## 10. Interview Questions

### Q1: What is Spark architecture?

Spark consists of Driver, Executors, Cluster Manager, Storage layer, and Shuffle system. It converts transformations into DAGs and executes them in distributed stages across executors.

---

### Follow-up: Why is Driver important?

Driver is responsible for:
- DAG creation
- Scheduling
- Execution coordination

Without Driver, distributed execution cannot be managed.

---

### Follow-up: What happens if Driver fails?

The entire job fails because execution state exists only in Driver memory.

---

### Q2: Why is Spark faster than Hadoop?

Because Spark:
- avoids disk writes between stages
- uses in-memory computation
- optimizes execution globally using DAG and Catalyst

---

### Follow-up: Why is shuffle expensive?

Because it involves:
- disk IO
- network transfer
- serialization
- sorting and merging

---

### Q3 (Staff level): Where does Spark break at scale?

Spark breaks due to:
- shuffle explosion
- data skew
- driver memory pressure
- executor failures
- cloud IO bottlenecks

At scale, system bottlenecks dominate compute performance.

---

## Key Takeaway

Spark is a distributed execution system where:
- Driver = control plane
- Executors = compute plane
- DAG = execution blueprint
- Shuffle = data movement layer
- Lineage = recovery mechanism

Most production issues come from data distribution and system constraints, not code logic.
