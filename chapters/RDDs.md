Chapter 2 — RDD Internals
Sections
2.1 RDD Architecture
2.2 Transformations
2.3 Actions
2.4 RDD Persistence
2.5 Partitioning in RDDs
2.6 Lineage Graphs
2.7 Fault Recovery
2.8 Custom Partitioners
2.9 Functional Execution Model
2.10 RDD Optimization
2.11 RDD Memory Handling
2.12 RDD Serialization
2.13 RDD vs DataFrames
2.14 RDD Use Cases
2.15 RDD Troubleshooting
----

# 2.1 RDD Architecture (Staff Level Deep Dive for FAANG Interviews)

# Core Concept

RDD (Resilient Distributed Dataset) is the foundational abstraction in Spark that represents a fault-tolerant, immutable, partitioned collection of data distributed across a cluster.

At Staff level understanding, RDD is not just a data structure. It is a **distributed execution model built on lineage based recomputation, partition based parallelism, and lazy evaluation of transformations across executors**.

RDD exists to solve three core distributed systems problems:

1. Fault tolerance without expensive replication
2. Efficient distributed parallel computation
3. Deterministic recomputation of lost data

---

# Internal Architecture of RDD

RDD is composed of four core internal components:

## 1 Partitions

RDD is physically divided into partitions.

Each partition:

- Represents a subset of data
- Is processed independently
- Is mapped to a single task
- Executes on a single executor core

This means parallelism is directly controlled by number of partitions.

At scale:

- More partitions means higher parallelism
- Too many partitions increases scheduling overhead
- Too few partitions reduces cluster utilization

---

## 2 Dependencies (Lineage Graph)

RDDs are connected through a dependency graph known as lineage.

There are two types of dependencies:

### Narrow Dependency
Each child partition depends on exactly one parent partition.

Characteristics:
- No shuffle required
- Fast execution
- Pipelined processing

Example operations:
map, filter, mapPartitions

---

### Wide Dependency
Each child partition depends on multiple parent partitions.

Characteristics:
- Requires shuffle
- Expensive network and disk IO
- Creates stage boundary

Example operations:
groupBy, join, distinct

---

Lineage is the backbone of Spark fault tolerance.

---

## 3 Computation Logic

RDD does not store data.

Instead, it stores transformation logic.

Example:

```python
rdd = inputRDD.map(lambda x: x * 2)
```

Here Spark stores:

- transformation function
- dependency on inputRDD

Not the actual computed data.

This enables lazy execution.

---

## 4 Metadata

RDD maintains metadata such as:

- partitioning scheme
- preferred execution locations
- storage level if cached
- lineage information

This metadata helps Spark optimize execution and data placement.

---

# Execution Flow of RDD

When an RDD operation is triggered:

## Step 1 Lineage Construction
Transformations are recorded as a logical lineage graph.

## Step 2 DAG Formation
Spark converts lineage into a DAG of execution stages.

## Step 3 Stage Division
Wide dependencies introduce stage boundaries.

## Step 4 Task Creation
Each partition becomes one task.

## Step 5 Execution on Executors
Tasks are sent to executors and executed in parallel.

---

# Internal Execution Behavior

RDD execution is lazy.

Nothing runs until an action is triggered.

When action is called:

- Spark walks lineage graph
- Builds execution plan
- Schedules tasks
- Executes partition computation on executors

Each execution recomputes partitions unless cached.

---

# Failure Handling in RDD Architecture

RDD is designed for fault tolerance using lineage.

## Case 1 Partition Failure

If a partition is lost:

- Spark traces lineage backward
- Recomputes only missing partition
- No full job restart required

---

## Case 2 Executor Failure

If executor fails:

- All tasks on that executor are lost
- Spark reschedules tasks elsewhere
- Lost partitions are recomputed using lineage

---

## Case 3 Driver Failure

If driver fails:

- Entire lineage graph is lost
- Job cannot recover
- Execution must restart from scratch

---

# Production Scenario (Critical Interview Pattern)

## Scenario: Repeated Full Pipeline Recomputations

A Spark job using RDD runs multiple actions on same dataset.

Problem:
Job runs repeatedly slower over time.

Root Cause:
RDD lineage is recomputed multiple times because:

- no caching used
- multiple actions trigger full recomputation

Internal Behavior:
Each action triggers full DAG traversal and recomputation from source.

Fix:
- persist intermediate RDDs
- use cache strategically
- break lineage using checkpointing

---

# RDD Memory Behavior

RDD does not guarantee memory residency.

Storage levels include:

- memory only
- memory and disk
- disk only
- serialized format

If memory is insufficient:

- data spills to disk
- recomputation may occur

---

# RDD Serialization Internals

RDD data is serialized when:

- sent to executors
- stored in memory
- persisted to disk

Poor serialization leads to:

- high network overhead
- slow execution
- increased GC pressure

---

# Optimization Techniques

## 1 Partition Optimization
Balance between parallelism and scheduling overhead.

## 2 Minimize Wide Dependencies
Reduce shuffle operations wherever possible.

## 3 Cache Intermediate RDDs
Avoid recomputation in repeated actions.

## 4 Reduce Lineage Depth
Use checkpointing for long transformation chains.

---

# Spark UI Perspective

RDD execution is visible in:

- DAG visualization
- stage breakdown
- shuffle metrics
- storage tab (cached RDDs)
- task execution timeline

---

# Interview Questions (Deep Level)

---

## Q1: What is RDD architecture in Spark

RDD architecture is a distributed execution model based on partitioned data, lazy transformation logic, and lineage based fault tolerance that enables scalable computation without data replication.

---

## Q2: Why is RDD called resilient

Because it can recover lost partitions by recomputing them using lineage instead of storing redundant copies of data.

---

## Q3: What happens when an RDD partition is lost

Spark recomputes only that partition by tracing lineage back to source transformations.

---

## Q4: Why is RDD immutable

Immutability ensures deterministic computation and simplifies distributed fault recovery.

---

## Q5: What is the biggest limitation of RDD

Lack of query optimization and higher execution overhead compared to DataFrames.

---

## Q6: What happens if lineage becomes too long

Driver memory pressure increases and recomputation cost becomes high.

---

## Q7: When should RDD be used in production

When low level control, custom partitioning, or complex transformations are required.

---

## Q8: How does RDD achieve fault tolerance

Through lineage based recomputation of lost partitions.

---

## Q9: Why is RDD slower than DataFrames

Because it lacks Catalyst optimization and Tungsten execution engine.

---

## Q10: What is core idea behind RDD architecture

To enable distributed computation using immutable data and lineage based recovery instead of replication.

---

# Staff Level Mental Model

RDD architecture is a distributed computation model where data is represented as immutable partitions, transformations define execution logic, and fault tolerance is achieved through lineage based recomputation, enabling scalable distributed processing but requiring careful control of partitioning, caching, and dependency complexity for production scale performance.

