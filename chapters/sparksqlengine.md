
# Chapter 4 Spark SQL Engine

---

# 4.1 Spark SQL Architecture

## 1 Query Ingestion Layer

A Spark SQL query enters the system through either
- spark.sql SELECT ...
- DataFrame API df.select.filter

Both are internally converted into a unified representation called a Logical Plan

At this stage
- No execution happens
- No data is read
- No schema validation occurs

Spark only interprets the query as intent not execution

---

## 2 SQL Parsing Abstract Syntax Tree AST

Spark first performs lexical and syntactic parsing of the SQL string

Example

SELECT name FROM users WHERE age > 30

This is converted into an Abstract Syntax Tree AST containing nodes such as
- SELECT
- PROJECT
- FILTER
- TABLE REFERENCE

### What happens here
- Syntax validation is performed
- Query structure is built
- No schema resolution happens
- No optimization happens

This is purely a grammar level transformation

---

## 3 Analysis Phase Catalyst Analyzer

The AST is passed into the Catalyst Analyzer which resolves semantic meaning using Spark Catalog Hive Metastore or Unity Catalog in Databricks

### Responsibilities
- Resolve table names
- Resolve column references
- Validate functions
- Infer data types

### Failures at this stage
- AnalysisException cannot resolve column
- Table or view not found

### Output
Analyzed Logical Plan

This is the first stage where Spark understands actual data structures

---

## 4 Logical Plan Construction

The analyzed plan is transformed into a Logical Plan Tree

Example

Project name
  Filter age greater than 30
    Relation users

### Characteristics
- Defines WHAT to do
- No execution strategy
- No shuffle decisions
- No partitioning logic

This is a relational algebra representation

---

## 5 Catalyst Optimizer Core Intelligence Layer

Catalyst applies rule based and cost based transformations on the logical plan

---

### Rule Based Optimizations

#### Predicate Pushdown
Filters are pushed closer to the data source scan level

Benefits
- Reduces rows read from storage

Limitations
- Fails for non pushable expressions like UDFs or complex logic

---

#### Column Pruning
Only required columns are read from storage

Benefits
- Reduces I O and memory usage

Limitations
- Fails when SELECT star is used or schema is not projected properly

---

#### Constant Folding
Compile time evaluation of constant expressions

Example

WHERE 10 plus 20 greater than 25

Becomes

WHERE TRUE

Benefits
- Removes runtime computation

Limitations
- Works only for deterministic expressions

---

#### Filter Reordering
Filters are reordered based on selectivity

Benefits
- More selective filters reduce data early

Risk
- Wrong estimation can degrade performance

---

#### Join Reordering CBO enabled
Join order is rearranged to minimize intermediate data size

Benefits
- Reduces shuffle cost significantly

Dependency
- Requires accurate statistics

---

### Cost Based Optimization CBO

Uses
- table statistics
- cardinality estimation
- number of distinct values

To choose optimal join and execution strategies

---

### Output
Optimized Logical Plan

---

## 6 Physical Planning

The physical planner converts the optimized logical plan into executable strategies

### Key decisions
- BroadcastHashJoin or SortMergeJoin
- ShuffleHashJoin selection
- HashAggregate or SortAggregate
- Partition pruning strategy
- File scan strategy

Spark may generate multiple physical plans and selects the lowest cost one

---

## 7 Execution Engine DAG and Tungsten Runtime

Once physical plan is finalized

### Execution stack
- DAG Scheduler
- Task Scheduler
- Executors

### Tungsten optimizations
- UnsafeRow binary format
- Whole Stage Code Generation
- Vectorized column execution
- Reduced JVM object overhead

---

## 8 Distributed Execution Model

Execution is split into stages

- Narrow transformations stay in same stage
- Wide transformations create shuffle boundaries

### Shuffle flow
- Shuffle Write on map side
- Shuffle Service
- Shuffle Read on reduce side

Final aggregation happens on the driver

---

# Tradeoffs

---

## 1 Abstraction vs Control

Spark automatically optimizes execution but reduces direct control over
- join strategy
- shuffle behavior
- execution order

---

## 2 Dependency on Statistics

Performance depends heavily on
- accuracy of table statistics
- correctness of data distribution assumptions

Bad statistics lead to poor execution plans

---

## 3 Debug Complexity

Issues can originate from multiple layers
- parsing layer
- analyzer layer
- optimizer layer
- execution layer

Debugging requires inspecting multiple plans and Spark UI

---

# Production Scenarios

---

## Scenario 1 Sudden Query Slowdown

### Symptoms
Query performance drops without code change

### Root Causes
- physical plan changed from broadcast join to shuffle join
- stale or missing statistics
- AQE changed execution strategy

### Debug Steps
- compare EXPLAIN FORMATTED before and after
- inspect join strategy changes
- check Spark UI shuffle stages
- validate ANALYZE TABLE statistics

### Fix
- refresh table statistics
- force broadcast join if valid
- tune AQE settings
- optimize partitioning strategy

---

## Scenario 2 Shuffle Explosion

### Symptoms
- slow stage execution
- high shuffle read and write
- executor memory pressure

### Root Causes
- inefficient join strategy
- missing predicate pushdown
- skewed data
- excessive wide transformations

### Fix
- push filters earlier in query
- use broadcast joins
- enable AQE skew handling
- optimize partitioning

---

## Scenario 3 Full Table Scan Unexpectedly

### Root Causes
- predicate pushdown not applied
- unsupported filter expressions
- CSV or JSON format used
- UDF blocking optimization

### Fix
- check PushedFilters in physical plan
- use Parquet or Delta format
- simplify filter logic
- avoid UDFs in filters

---

# Interview Questions

---

## Q1 Explain Spark SQL Architecture

Spark SQL architecture converts SQL into an optimized distributed execution plan using
- SQL parsing into AST
- Catalyst analysis
- logical plan construction
- Catalyst optimization
- physical planning
- Tungsten execution

---

## Q2 Why separate logical and physical plans

Logical plan defines what to do and physical plan defines how to execute it efficiently on a distributed system

---

## Q3 Why does the same query behave differently in production

Because execution depends on
- data statistics
- data distribution changes
- AQE decisions
- cluster state
- shuffle behavior

---

## Q4 Can Spark choose a wrong execution plan

Yes if statistics are missing or incorrect Catalyst may choose inefficient join or scan strategies

---

## Q5 How do you debug slow Spark SQL queries

- compare logical and physical plans
- analyze Spark UI stages
- inspect shuffle behavior
- validate statistics and join strategy

---

# Final Mental Model

Spark SQL is a distributed query compilation system not a direct execution engine

Every query goes through
- parsing AST creation
- analysis metadata resolution
- logical planning relational structure
- optimization Catalyst rewriting
- physical planning execution strategy
- runtime execution Tungsten and DAG engine

Performance depends on
- optimizer decisions
- statistics accuracy
- execution plan quality
- runtime data distribution
