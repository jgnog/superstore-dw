# Superstore Data Warehouse

This is a sample dbt project for educational purposes.

## Seeds

This project contains a CSV seed containing data for a date dimension table.

Import it into a table called `dim_date` in your data warehouse by running:

```
dbt seed
```

## Snapshots

The project contains snapshots used to track dimension attribute changes.
They use staging models as sources, so you need to update them before updating
the snapshots.

Update the staging models by running

```
dbt run --select staging
```

Update the snapshots by running

```
dbt snapshot
```

## Final run

To build the final models, simply run

```
dbt run --select final
```

## Tests

Finally, you can run the tests with

```
dbt test
```
