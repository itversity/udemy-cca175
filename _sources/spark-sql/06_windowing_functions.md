---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.12
    jupytext_version: 1.6.0
kernelspec:
  display_name: Apache Toree - Scala
  language: scala
  name: apache_toree_scala
---

# Windowing Functions

As part of this section we will primarily talk about Windowing Functions. These are also known as Analytic Functions in Databases like Oracle.

* Prepare HR Database
* Overview of Windowing Functions
* Aggregations using Windowing Functions
* Getting LEAD and LAG values
* Getting first and last values
* Ranking using Windowing Functions
* Understanding order of execution of SQL
* Overview of Nested Sub Queries
* Filtering - Window Function Results

```{code-cell} scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.
    builder.
    config("spark.ui.port", "0").
    config("spark.sql.warehouse.dir", "/user/itversity/warehouse").
    enableHiveSupport.
    appName("Spark SQL - Windowing Functions").
    master("yarn").
    getOrCreate
```

```{code-cell} scala
%%sql
SET spark.sql.shuffle.partitions=2
```

## Prepare HR Database

Let us prepare HR database with **EMPLOYEES** Table. We will be using this for some of the examples as well as exercises related to Window Functions.

* Create Database **itversity_hr** (replace itversity with your OS User Name)
* Create table **employees** in **itversity_hr** database.
* Load data into the table.

First let us start with creating the database.

```{code-cell} scala
%%sql

DROP DATABASE itversity_hr CASCADE
```

```{code-cell} scala
%%sql

CREATE DATABASE itversity_hr
```

```{code-cell} scala
%%sql

USE itversity_hr
```

```{code-cell} scala
%%sql

SELECT current_database()
```

As the database is created, let us go ahead and add table to it.

```{code-cell} scala
%%sql

CREATE TABLE employees (
  employee_id     int,
  first_name      varchar(20),
  last_name       varchar(25),
  email           varchar(25),
  phone_number    varchar(20),
  hire_date       date,
  job_id          varchar(10),
  salary          decimal(8,2),
  commission_pct  decimal(2,2),
  manager_id      int,
  department_id   int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
```

Let us load the data and validate the table.

```{code-cell} scala
%%sql

LOAD DATA LOCAL INPATH '/data/hr_db/employees' 
INTO TABLE employees
```

```{code-cell} scala
%%sql

SELECT * FROM employees LIMIT 10
```

```{code-cell} scala
%%sql

SELECT employee_id, department_id, salary FROM employees LIMIT 10
```

```{code-cell} scala
%%sql

SELECT count(1) FROM employees
```

## Overview of Windowing Functions

Let us get an overview of Analytics or Windowing Functions in Spark SQL.

* Aggregate Functions (`sum`, `min`, `max`, `avg`)
* Window Functions (`lead`, `lag`, `first_value`, `last_value`)
* Rank Functions (`rank`, `dense_rank`, `row_number` etc)
* For all the functions we use `OVER` clause.
* For aggregate functions we typically use `PARTITION BY`
* For ranking and windowing functions we might use `ORDER BY sorting_column` or `PARTITION BY partition_column ORDER BY sorting_column`.

```{code-cell} scala
%%sql

USE itversity_hr
```

```{code-cell} scala
%%sql

SELECT employee_id, department_id, salary FROM employees LIMIT 10
```

```{code-cell} scala
%%sql

SELECT employee_id, department_id, salary,
    count(1) OVER (PARTITION BY department_id) AS employee_count,
    rank() OVER (ORDER BY salary DESC) AS rnk,
    lead(employee_id) OVER (PARTITION BY department_id ORDER BY salary DESC) AS lead_emp_id,
    lead(salary) OVER (PARTITION BY department_id ORDER BY salary DESC) AS lead_emp_sal
FROM employees
ORDER BY employee_id
```

## Aggregations using Windowing Functions

Let us see how we can perform aggregations with in a partition or group using Windowing/Analytics Functions.

* For simple aggregations where we have to get grouping key and aggregated results we can use **GROUP BY**.
* If we want to get the raw data along with aggregated results, then using **GROUP BY** is not possible or overly complicated.
* Using aggregate functions with **OVER** Clause not only simplifies the process of writing query, but also better with respect to performance.
* Let us take an example of getting employee salary percentage when compared to department salary expense.

```{code-cell} scala
%%sql

USE itversity_hr
```

```{code-cell} scala
%%sql

SELECT employee_id, department_id, salary 
FROM employees 
ORDER BY department_id, salary
LIMIT 10
```

> Let us write the query using `GROUP BY` approach.

```{code-cell} scala
%%sql

SELECT department_id,
       sum(salary) AS department_salary_expense
FROM employees
GROUP BY department_id
ORDER BY department_id
```

```{code-cell} scala
%%sql

SELECT e.employee_id, e.department_id, e.salary,
       ae.department_salary_expense,
       ae.avg_salary_expense
FROM employees e JOIN (
     SELECT department_id, 
            sum(salary) AS department_salary_expense,
            avg(salary) AS avg_salary_expense
     FROM employees
     GROUP BY department_id
) ae
ON e.department_id = ae.department_id
ORDER BY department_id, salary
```

> Let us see how we can get it using Analytics/Windowing Functions. 

* We can use all standard aggregate functions such as `count`, `sum`, `min`, `max`, `avg` etc.

```{code-cell} scala
%%sql

SELECT e.employee_id, e.department_id, e.salary,
       sum(e.salary) 
         OVER (PARTITION BY e.department_id)
         AS department_salary_expense
FROM employees e
ORDER BY e.department_id
```

```{code-cell} scala
%%sql

SELECT e.employee_id, e.department_id, e.salary,
    sum(e.salary) OVER (PARTITION BY e.department_id) AS sum_sal_expense,
    avg(e.salary) OVER (PARTITION BY e.department_id) AS avg_sal_expense,
    min(e.salary) OVER (PARTITION BY e.department_id) AS min_sal_expense,
    max(e.salary) OVER (PARTITION BY e.department_id) AS max_sal_expense,
    count(e.salary) OVER (PARTITION BY e.department_id) AS cnt_sal_expense
FROM employees e
ORDER BY e.department_id
```

### Create tables to get daily revenue

Let us create couple of tables which will be used for the demonstrations of Windowing and Ranking functions.

* We have **ORDERS** and **ORDER_ITEMS** tables.
* Let us take care of computing daily revenue as well as daily product revenue.
* As we will be using same data set several times, let us create the tables to pre compute the data.
* **daily_revenue** will have the **order_date** and **revenue**, where data is aggregated using **order_date** as partition key.
* **daily_product_revenue** will have **order_date**, **order_item_product_id** and **revenue**. In this case data is aggregated using **order_date** and **order_item_product_id** as partition keys.

Let us create table to compute daily revenue.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

DROP TABLE IF EXISTS daily_revenue
```

```{code-cell} scala
%%sql

CREATE TABLE daily_revenue
AS
SELECT o.order_date,
       round(sum(oi.order_item_subtotal), 2) AS revenue
FROM orders o JOIN order_items oi
ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY o.order_date
```

```{code-cell} scala
%%sql

SELECT * 
FROM daily_revenue
ORDER BY order_date
LIMIT 10
```

Let us create table to compute daily product revenue.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

DROP TABLE IF EXISTS daily_product_revenue
```

```{code-cell} scala
%%sql

CREATE TABLE daily_product_revenue
AS
SELECT o.order_date,
       oi.order_item_product_id,
       round(sum(oi.order_item_subtotal), 2) AS revenue
FROM orders o JOIN order_items oi
ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY o.order_date, oi.order_item_product_id
```

```{code-cell} scala
%%sql

SELECT * 
FROM daily_product_revenue
ORDER BY order_date, order_item_product_id
LIMIT 10
```

## Getting LEAD and LAG values

Let us understand LEAD and LAG functions to get column values from following or prior rows.

Here is the example where we can get prior or following records based on **ORDER BY** Clause.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

SELECT * FROM daily_revenue
ORDER BY order_date DESC
LIMIT 10
```

```{code-cell} scala
%%sql

SELECT t.*,
  lead(order_date) OVER (ORDER BY order_date DESC) AS prior_date,
  lead(revenue) OVER (ORDER BY order_date DESC) AS prior_revenue
FROM daily_revenue t
ORDER BY order_date DESC
LIMIT 10
```

We can also pass number of rows as well as default values for nulls as arguments.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

SELECT t.*,
  lead(order_date, 7) OVER (ORDER BY order_date DESC) AS prior_date,
  lead(revenue, 7) OVER (ORDER BY order_date DESC) AS prior_revenue
FROM daily_revenue t
ORDER BY order_date DESC
LIMIT 10
```

```{code-cell} scala
%%sql

SELECT t.*,
  lead(order_date, 7) OVER (ORDER BY order_date DESC) AS prior_date,
  lead(revenue, 7) OVER (ORDER BY order_date DESC) AS prior_revenue
FROM daily_revenue t
ORDER BY order_date
LIMIT 10
```

```{code-cell} scala
%%sql

SELECT t.*,
  lead(order_date, 7) OVER (ORDER BY order_date DESC) AS prior_date,
  lead(revenue, 7, 0) OVER (ORDER BY order_date DESC) AS prior_revenue
FROM daily_revenue t
ORDER BY order_date
LIMIT 10
```

Let us see how we can get prior or following records with in a group based on particular order.

Here is the example where we can get prior or following records based on **PARTITION BY** and then **ORDER BY** Clause.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

DESCRIBE daily_product_revenue
```

```{code-cell} scala
%%sql

SELECT * FROM daily_product_revenue LIMIT 10
```

```{code-cell} scala
%%sql

SELECT t.*,
  LEAD(order_item_product_id) OVER (
    PARTITION BY order_date 
    ORDER BY revenue DESC
  ) next_product_id,
  LEAD(revenue) OVER (
    PARTITION BY order_date 
    ORDER BY revenue DESC
  ) next_revenue
FROM daily_product_revenue t
ORDER BY order_date, revenue DESC
LIMIT 100
```

We can also pass number of rows as well as default values for nulls as arguments.

```{code-cell} scala
%%sql

SELECT t.*,
  LEAD(order_item_product_id) OVER (
    PARTITION BY order_date ORDER BY revenue DESC
  ) next_product_id,
  LEAD(revenue, 1, 0) OVER (
    PARTITION BY order_date ORDER BY revenue DESC
  ) next_revenue
FROM daily_product_revenue t
LIMIT 100
```

## Getting first and last values

Let us see how we can get first and last value based on the criteria. We can also use min or max as well.

Here is the example of using first_value.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

SELECT t.*,
  first_value(order_item_product_id) OVER (
    PARTITION BY order_date ORDER BY revenue DESC
  ) first_product_id,
  first_value(revenue) OVER (
    PARTITION BY order_date ORDER BY revenue DESC
  ) first_revenue
FROM daily_product_revenue t
ORDER BY order_date, revenue DESC
LIMIT 100
```

Let us see an example with last_value. While using last_value we need to specify **ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING/PRECEEDING**. By default it uses

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

SELECT t.*,
  last_value(order_item_product_id) OVER (
    PARTITION BY order_date ORDER BY revenue
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  ) last_product_id,
  last_value(revenue) OVER (
    PARTITION BY order_date ORDER BY revenue
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  )  last_revenue
FROM daily_product_revenue AS t
ORDER BY order_date, revenue DESC
LIMIT 100
```

## Ranking using Windowing Functions

Let us see how we can get sparse ranks using **rank** function.

* If we have to get ranks globally, we just need to specify **ORDER BY**
* If we have to get ranks with in a key then we need to specify **PARTITION BY** and then **ORDER BY**.
* By default **ORDER BY** will sort the data in ascending order. We can change the order by passing **DESC** after order by.

Here is an example to assign sparse ranks using daily_product_revenue with in each day based on revenue.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

SELECT t.*,
  rank() OVER (
    PARTITION BY order_date
    ORDER BY revenue DESC
  ) AS rnk
FROM daily_product_revenue t
ORDER BY order_date, revenue DESC
LIMIT 100
```

Here is another example to assign sparse ranks using employees data set with in each department.

```{code-cell} scala
%%sql

USE itversity_hr
```

```{code-cell} scala
%%sql

SELECT
  employee_id,
  department_id,
  salary,
  rank() OVER (
    PARTITION BY department_id
    ORDER BY salary DESC
  ) rnk,
  dense_rank() OVER (
    PARTITION BY department_id
    ORDER BY salary DESC
  ) drnk,
  row_number() OVER (
    PARTITION BY department_id
    ORDER BY salary DESC
  ) rn
FROM employees
ORDER BY department_id, salary DESC
```

```{code-cell} scala
%%sql

SELECT * FROM employees ORDER BY salary LIMIT 10
```

```{code-cell} scala
%%sql

SELECT employee_id, salary,
    dense_rank() OVER (ORDER BY salary DESC) AS drnk
FROM employees
```

Let us understand the difference between **rank**, **dense_rank** and **row_number**.

* We can either of the functions to generate ranks when the rank field does not have duplicates.
* When rank field have duplicates then row_number should not be used as it generate unique number for each record with in the partition.
* **rank** will skip the ranks in between if multiple people get the same rank while **dense_rank** continue with the next number.

+++

## Understanding order of execution of SQL

Let us review the order of execution of SQL. First let us review the order of writing the query.

1. **SELECT**
2. **FROM**
3. **JOIN** or **OUTER JOIN** with **ON**
4. **WHERE**
5. **GROUP BY** and optionally **HAVING**
6. **ORDER BY**

Let us come up with a query which will compute daily revenue using COMPLETE or CLOSED orders and also ordered by order_date.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

SELECT o.order_date,
  round(sum(oi.order_item_order_id), 2) AS revenue
FROM orders o JOIN order_items oi
ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY o.order_date
ORDER BY o.order_date
LIMIT 10
```

```{code-cell} scala
%%sql

SELECT o.order_date,
    round(sum(oi.order_item_order_id), 2) AS revenue
FROM orders o JOIN order_items oi
ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY o.order_date
    HAVING revenue >= 2000000
ORDER BY order_date
LIMIT 10
```

However order of execution is different.

1. **FROM**
2. **JOIN** or **OUTER JOIN** with **ON**
3. **WHERE**
4. **GROUP BY** and optionally **HAVING**
5. **SELECT**
6. **ORDER BY**

As **SELECT** is executed before **ORDER BY** Clause, we will not be able to refer the aliases in **SELECT** in other clauses except for **ORDER BY**.

+++

## Overview of Nested Sub Queries

Let us recap about Nested Sub Queries.

* We typically have Nested Sub Queries in **FROM** Clause.
* We need to provide alias to the Nested Sub Queries in **FROM** Clause in Hive.
* We use nested queries quite often over queries using Analytics/Windowing Functions

```{code-cell} scala
%%sql

SELECT * FROM (SELECT current_date) AS q
```

Let us see few more examples with respected to Nested Sub Queries.

```{code-cell} scala
%%sql

SELECT * FROM (
  SELECT order_date, count(1) AS order_count
  FROM orders
  GROUP BY order_date
) q
LIMIT 10
```

```{code-cell} scala
%%sql

SELECT * FROM (
  SELECT order_date, count(1) AS order_count
  FROM orders
  GROUP BY order_date
) q
WHERE q.order_count > 0
```

* We can achieve using HAVING clause (no need to be nested to filter)

+++

## Filtering - Window Function Results

Let us understand how to filter on top of results of Window Functions.

* We can use Window Functions only in **SELECT** Clause.
* If we have to filter based on Window Function results, then we need to use Nested Sub Queries.
* Once the query is nested, we can apply filter using aliases of the Window Functions.

Here is the example where we can filter data based on Window Functions.

```{code-cell} scala
%%sql

SELECT * FROM (
  SELECT t.*,
    dense_rank() OVER (
      PARTITION BY order_date
      ORDER BY revenue DESC
    ) AS drnk
  FROM daily_product_revenue t
) q
WHERE drnk <= 5
ORDER BY q.order_date, q.revenue DESC
LIMIT 100
```

### Ranking and Filtering - Recap

Let us recap the procedure to get top 5 orders by revenue for each day.

* We have our original data in **orders** and **order_items**
* We can pre-compute the data or create a view with the logic to generate **daily product revenue**
* Then, we have to use the view or table or even nested query to compute rank
* Once the ranks are computed, we need to nest it to filter based up on our requirement.
* Let us see using the query example.

Let us come up with the query to compute daily product revenue.

```{code-cell} scala
%%sql

USE itversity_retail
```

```{code-cell} scala
%%sql

DESCRIBE orders
```

```{code-cell} scala
%%sql

DESCRIBE order_items
```

```{code-cell} scala
%%sql

SELECT o.order_date,
       oi.order_item_product_id,
       round(sum(oi.order_item_subtotal), 2) AS revenue
FROM orders o JOIN order_items oi
ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY o.order_date, oi.order_item_product_id
ORDER BY o.order_date, revenue DESC
LIMIT 100
```

Let us compute the rank for each product with in each date using revenue as criteria.

```{code-cell} scala
%%sql

SELECT q.*,
  rank() OVER (
    PARTITION BY order_date
    ORDER BY revenue DESC
  ) AS rnk
FROM (SELECT o.order_date,
        oi.order_item_product_id,
        round(sum(oi.order_item_subtotal), 2) AS revenue
      FROM orders o JOIN order_items oi
      ON o.order_id = oi.order_item_order_id
      WHERE o.order_status IN ('COMPLETE', 'CLOSED')
      GROUP BY o.order_date, oi.order_item_product_id) q
ORDER BY order_date, revenue DESC
LIMIT 35
```

Now let us see how we can filter the data.

```{code-cell} scala
%%sql

SELECT * FROM (SELECT q.*,
  dense_rank() OVER (
    PARTITION BY order_date
    ORDER BY revenue DESC
  ) AS drnk
FROM (SELECT o.order_date,
        oi.order_item_product_id,
        round(sum(oi.order_item_subtotal), 2) AS revenue
      FROM orders o JOIN order_items oi
      ON o.order_id = oi.order_item_order_id
      WHERE o.order_status IN ('COMPLETE', 'CLOSED')
      GROUP BY o.order_date, oi.order_item_product_id) q) q1
WHERE drnk <= 5
ORDER BY order_date, revenue DESC
LIMIT 35
```

```{code-cell} scala
spark.sql("DESCRIBE daily_product_revenue").show(false)
```

```{code-cell} scala
%%sql

SELECT * FROM (SELECT dpr.*,
  dense_rank() OVER (
    PARTITION BY order_date
    ORDER BY revenue DESC
  ) AS drnk
FROM daily_product_revenue AS dpr)
WHERE drnk <= 5
ORDER BY order_date, revenue DESC
LIMIT 35
```

```{code-cell} scala

```
