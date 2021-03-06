
# Tuple(T1, T2, ...)

A tuple of elements, each having an individual [type](index.md#data_types).

Tuples are used for temporary column grouping. Columns can be grouped when an IN expression is used in a query, and for specifying certain formal parameters of lambda functions. For more information, see the sections [IN operators](../query_language/select.md) and [Higher order functions](../query_language/functions/higher_order_functions.md).

Tuples can be the result of a query. In this case, for text formats other than JSON, values are comma-separated in brackets. In JSON formats, tuples are output as arrays (in square brackets).

## Creating a tuple

You can use a function to create a tuple:

```sql
tuple(T1, T2, ...)
```

Example of creating a tuple:

```sql
SELECT tuple(1,'a') AS x, toTypeName(x)
```
```text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

## Working with data types

When creating a tuple on the fly, ClickHouse automatically detects the type of each argument as the minimum of the types which can store the argument value. If the argument is [NULL](../query_language/syntax.md#null-literal), the type of the tuple element is [Nullable](nullable.md).

Example of automatic data type detection:

```sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```
```text
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```


[Original article](https://clickhouse.tech/docs/en/data_types/tuple/) <!--hide-->
