# SQL Join Benchmark Patterns (Lua Module)

## Overview

This Lua module defines a comprehensive library of SQL query patterns designed for database benchmarking and performance testing. It focuses heavily on **JOIN operations**, ranging from simple single-table joins to complex, multi-level hierarchical aggregations.

## Data Model & Schema Context

To understand the queries, one must understand the underlying data model implied by the code. The schema simulates a hierarchical structure:

* **Main Table (`m`):** The central fact table containing grouping keys (`continent`, `year_field`, `enum_field`) and foreign keys (`l1_id`).
* **Level Tables (`level1` to `level5`):** A hierarchy of dimension tables.
* `level1` connects to `Main`.
* `level2` connects to `level1`, and so on up to `level5`.
* These tables contain metrics (`record_value`) and status flags (`record_status`).

*** Main Table definition: ***
```sql
+-----------------+----------------------------------------+------+-----+-------------------+-------------------+
| Field           | Type                                   | Null | Key | Default           | Extra             |
+-----------------+----------------------------------------+------+-----+-------------------+-------------------+
| id              | bigint                                 | NO   | PRI | NULL              | auto_increment    |
| l1_id           | int                                    | YES  | MUL | NULL              |                   |
| l2_id           | int                                    | YES  | MUL | NULL              |                   |
| l3_id           | int                                    | YES  | MUL | NULL              |                   |
| l4_id           | int                                    | YES  | MUL | NULL              |                   |
| l5_id           | int                                    | YES  | MUL | NULL              |                   |
| small_number    | smallint                               | YES  |     | NULL              |                   |
| integer_number  | int                                    | YES  |     | NULL              |                   |
| myvalue         | bigint                                 | YES  |     | NULL              |                   |
| decimal_number  | decimal(10,2)                          | YES  |     | NULL              |                   |
| float_number    | float                                  | YES  |     | NULL              |                   |
| char_field      | char(10)                               | YES  |     | NULL              |                   |
| varchar_field   | varchar(255)                           | YES  |     | NULL              |                   |
| color           | varchar(50)                            | YES  | MUL | NULL              |                   |
| continent       | varchar(255)                           | YES  | MUL | NULL              |                   |
| uuid            | varchar(36)                            | YES  | UNI | NULL              |                   |
| uuid_bin        | binary(16)                             | YES  |     | NULL              |                   |
| text_field      | text                                   | YES  |     | NULL              |                   |
| datetime_field  | datetime                               | YES  | MUL | NULL              |                   |
| timestamp_field | timestamp                              | YES  | MUL | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
| year_field      | year                                   | YES  | MUL | NULL              |                   |
| binary_field    | binary(50)                             | YES  |     | NULL              |                   |
| varbinary_field | varbinary(255)                         | YES  |     | NULL              |                   |
| enum_field      | enum('active','inactive','pending')    | YES  | MUL | NULL              |                   |
| set_field       | set('read','write','execute','delete') | YES  | MUL | NULL              |                   |
| is_active       | tinyint(1)                             | YES  |     | 1                 |                   |
+-----------------+----------------------------------------+------+-----+-------------------+-------------------+
```
*** Table Level definition: *** 
```
+-----------------+-------------------------------------+------+-----+-------------------+-----------------------------------------------+
| Field           | Type                                | Null | Key | Default           | Extra                                         |
+-----------------+-------------------------------------+------+-----+-------------------+-----------------------------------------------+
| id              | bigint                              | NO   | PRI | NULL              | auto_increment                                |
| continent       | varchar(45)                         | NO   | MUL | NULL              |                                               |
| parent_id       | bigint                              | YES  | MUL | NULL              |                                               |
| time_accessed   | timestamp                           | YES  | MUL | CURRENT_TIMESTAMP | DEFAULT_GENERATED on update CURRENT_TIMESTAMP |
| l1_id           | int                                 | YES  |     | NULL              |                                               |
| l2_id           | int                                 | YES  | MUL | NULL              |                                               |
| l3_id           | int                                 | YES  | MUL | NULL              |                                               |
| l4_id           | int                                 | YES  | MUL | NULL              |                                               |
| l5_id           | int                                 | YES  | MUL | NULL              |                                               |
| record_name     | char(36)                            | YES  |     | NULL              |                                               |
| record_code     | char(5)                             | YES  |     | NULL              |                                               |
| record_value    | bigint                              | YES  |     | NULL              |                                               |
| record_status   | enum('active','inactive','pending') | YES  | MUL | NULL              |                                               |
| record_priority | int                                 | NO   | MUL | NULL              |                                               |
+-----------------+-------------------------------------+------+-----+-------------------+-----------------------------------------------+
```
## query_map Configuration

The script organizes queries into specific Lua tables based on their SQL logic. These tables are eventually merged into a master `query_map`.

### 1. Inner Join Queries (`inner_queries`)

Tests standard relational intersection.

* **PK Joins:** Joins performed on Primary Keys (optimized paths).
* **Index Joins:** Joins performed on non-primary indexes (`parent_id`).
* **Group By (GB):** Aggregates data (`COUNT`, `SUM`) based on columns in the main table.
* **Forcing Order:** Uses Optimizer Hints (`/*+ JOIN_ORDER(...) */`) to force the database to execute joins in a specific sequence, bypassing the standard optimizer decision.
* **Straight Join:** Uses `STRAIGHT_JOIN` to force the join order to match the order tables appear in the query.

### 2. Left Join Queries (`left_queries`)

Tests left outer joins, retrieving all records from the left table (`Main` or `Level`) and matching records from the right.

* **Multi-Level:** Chains joins from Main  Level 1  ...  Level 5.
* **Exclusion Testing:** Includes "Anti-Join" patterns using `IS NULL` to find records in the main table that have *no* matching record in the child table.

### 3. Right Join Queries (`right_queries`)

Tests right outer joins.

* **Reverse Hierarchy:** Often starts from `level5` or `level1` and joins "backwards" to the Main table.
* Useful for testing if the optimizer handles right joins differently or converts them to left joins internally.

### 4. Subquery Joins (`subquery_queries`)

Tests the performance of joining against derived tables (subqueries in the `FROM` clause).

* **Materialization:** Forces the database to materialize a result set (e.g., filtering `Main` first) before joining to the hierarchy.

### 5. Semi & Anti-Joins (`semi_anti_condition_queries`)

Tests conditional existence without returning the joined data.

* **Semi-Join (`EXISTS`):** "Select rows where related rows exist."
* **Anti-Join (`NOT EXISTS`):** "Select rows where related rows do NOT exist."
* **Conditional:** Uses `CASE WHEN` statements within aggregations to calculate percentages based on status flags.

### 6. Write Operations (`insert_update_delete_queries`)

Tests complex `UPDATE` statements involving joins.

* Calculates a sum from the hierarchy (`level1`...`level5`) and updates a value in the main table based on that aggregation.

## Code Mechanics

### Dynamic String Formatting

The SQL strings use Lua's format specifiers to be dynamic at runtime:

* `%s%u`: Represents the Table Name + Table Number (e.g., `sbtest1`).
* `%s`: Base name.
* `%u`: Table index.


* `'%s'`: Represents dynamic values for `WHERE` clauses (e.g., filtering by specific continents, colors, or status flags).

### Global Variables

The function `load_global_variables()` sets up the `INSERT` statements used to populate the tables with data compatible with these read queries.

### Helper Functions

* **`mergeMultiple(...)`**: A utility function that takes the disparate query category tables (`inner_queries`, `left_queries`, etc.) and combines them into a single `query_map`.
* **`all_joins` Generation**: The script iterates through the merged map, strips the `_query` suffix from the keys, and creates a flat list of test names (`all_joins`). This list is typically used by the benchmark runner to pick tests to execute.

## Usage Example

## License

Copyright (C) 2006-present Marco Tusa.
Distributed under the **GNU General Public License v2**.

