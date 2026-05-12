
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

- -----

# 4.2 SQL Parsing in Spark SQL Engine

---

## 1 What SQL Parsing Actually Means in Spark

SQL parsing is the first stage where Spark starts interpreting a query string. At this point Spark has no understanding of tables, data, or execution. It only sees a raw text query.

The purpose of parsing is to convert this text into a structured representation that Spark can reason about internally.

This structured representation is called an Abstract Syntax Tree or AST.

---

## 2 Where Parsing Fits in Spark SQL Pipeline

Before understanding parsing in isolation, it is important to place it in the full flow

SQL string
  Parsing into AST
  Analysis using Catalyst Analyzer
  Logical Plan creation
  Optimization via Catalyst
  Physical Plan selection
  Execution via DAG and Tungsten

Parsing is strictly a syntax level transformation step. It is not aware of schema or data.

---

## 3 How Spark Performs Parsing Internally

When a query is submitted, Spark uses a SQL parser module built on ANTLR grammar definitions.

The parser performs two key operations

Lexical analysis
This breaks the query into tokens such as keywords, identifiers, operators, and literals

Syntax analysis
This arranges tokens into a structured tree based on SQL grammar rules

---

## 4 Example of SQL Parsing Transformation

Input query

SELECT name FROM users WHERE age > 30

After parsing Spark generates an AST like structure

Select
  Project name
  From users
  Filter
    GreaterThan age 30

At this stage
- Spark does not know if users table exists
- Spark does not know if age column exists
- Spark does not know data types
- Spark does not optimize anything

It only understands structure and intent

---

## 5 Key Properties of AST in Spark

The Abstract Syntax Tree in Spark has the following properties

- It is immutable once created
- It is language independent internal representation
- It is purely syntactic, not semantic
- It is used as input for Catalyst Analyzer

AST is not executed and never touches data

---

## 6 Why Parsing Layer Exists

Parsing exists because Spark must separate human readable SQL from internal execution logic

This separation allows

- multiple APIs to reuse same engine (SQL, DataFrame, Dataset)
- consistent interpretation of queries
- safe transformation into optimized plans

Without parsing Spark would have to interpret raw strings at runtime which is not scalable

---

## 7 Failure Cases in Parsing Stage

Parsing failures occur before any optimization or execution

Common failures include

Syntax errors
Example missing keyword or invalid SQL structure

SELECT FROM users

This fails because column selection is missing

Malformed expressions
Incorrect operators or unbalanced brackets

SELECT name FROM users WHERE age >>

In such cases Spark throws parse exception before reaching Catalyst

---

## 8 Parsing vs Analysis Difference

A critical interview concept

Parsing only checks syntax
Analysis checks semantics

Parsing result
Structure is valid SQL grammar

Analysis result
Tables and columns are validated against metadata

Example

SELECT abc FROM unknown_table

Parsing passes
Analysis fails

This distinction is very important in production debugging

---

## 9 Spark SQL Parser Internals

Internally Spark uses

- ANTLR generated SQL grammar
- Catalyst parser wrapper
- Logical plan builder hooks

The parser converts SQL into a Tree structure which later becomes a Logical Plan through Catalyst rules

---

## 10 Real Production Behavior of Parsing

In production systems parsing is very fast and rarely a bottleneck

However parsing failures often appear due to

- dynamic SQL generation bugs
- incorrect column names from upstream systems
- malformed queries in ETL pipelines

Parsing errors are usually caught early in job submission stage before any cluster resources are consumed

---

## 11 Debugging Parsing Issues

When a query fails at parsing stage, debugging is straightforward compared to later stages

Steps include

- validate raw SQL string
- check syntax correctness
- ensure reserved keyword usage is correct
- verify string escaping in dynamic queries

Spark logs will show parse exception without job execution attempt

---

## 12 Production Scenario Deep Dive

Scenario

A production ETL job suddenly starts failing with SQL parse errors after upstream changes

Root cause possibilities

- schema column renamed upstream but query not updated
- dynamically generated SQL introduced syntax bug
- templated query substitution injected invalid characters

Fix approach

- log final rendered SQL before execution
- validate query generation layer separately
- add pre execution query linting step

Prevention

- enforce SQL validation in CI pipelines
- maintain schema contract between upstream and downstream systems
- avoid string concatenation based SQL generation

---

## 13 Interview Question Set

---

### Q1 What happens during SQL parsing in Spark

SQL parsing converts a raw SQL string into an Abstract Syntax Tree using lexical and syntactic analysis. It validates grammar but does not validate schema or execute data operations.

---

### Q2 What is the difference between parsing and analysis in Spark

Parsing validates SQL syntax and builds AST. Analysis resolves metadata like table names, column names, and data types using Spark catalog.

---

### Q3 Does parsing access data or schema

No. Parsing is purely syntactic and does not access any metadata or data.

---

### Q4 Why is parsing important in Spark architecture

Because it creates a structured representation of SQL that can be optimized and executed across multiple engines in a consistent way.

---

### Q5 What happens if parsing fails

Spark throws a parse exception and stops execution before any Catalyst optimization or execution begins.

---

## 14 Follow Up Interview Questions

---

### Q Why does Spark separate parsing from analysis

Because parsing is language grammar validation while analysis requires metadata access. Separating them improves modularity and performance.

---

### Q What is AST in Spark SQL

AST is a tree representation of SQL query structure created during parsing before semantic resolution.

---

### Q How would you debug a parsing failure in production

By inspecting raw SQL string, checking dynamically generated query templates, and validating syntax before submission.

---

### Q Can parsing affect query performance

Not directly. Parsing is fast and only affects startup time, not execution performance.

---

## 15 Key Mental Model

SQL parsing in Spark is the process of converting a raw SQL string into a structured Abstract Syntax Tree using grammar rules. This step is purely syntactic and acts as the foundation for Catalyst analysis and optimization layers. It does not interact with data or schema but ensures that the query structure is valid before deeper semantic processing begins.
