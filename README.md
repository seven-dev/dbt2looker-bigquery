# dbt2looker-bigquery (active as of 13.9.2024)

Use `dbt2looker-bigquery` to generate Looker view files automatically from dbt models in Bigquery.

This is a fork of dbt2looker that is specific to bigquery.
The intention is to allow one to define most of the simple and tedious lookml settings in dbt.
That way the lookml code gets less bloated, and can be more focused on advanced metrics and explores.

Want a deeper integration between dbt and your BI tool?
You should also checkout [Lightdash - the open source alternative to Looker](https://github.com/lightdash/lightdash)

**Features**

- Warehouses: BigQuery

- **Column descriptions** synced to looker
- **Dimension** for each column in dbt model
- **Define Dimensions** define common lookml settings in dbt like label, group label, hidden
- **Opinionated Primary key** automatically set the first column to be the primary key, and hide it.
- **Create explores for structs** automatically generate explores for complex tables with structs and arrays in them.
- **Dimension groups** for datetime/timestamp/date columns
- **Measures** defined through dbt column `metadata` [see below](#defining-measures)

## Quickstart

Run `dbt2looker` in the root of your dbt project _after compiling looker docs_.
(dbt2looker-bigquery uses docs to infer types and such)

**Generate Looker view files for all models:**

```shell
dbt docs generate
dbt2looker
```

**Generate Looker view files for all models tagged `prod`**

```shell
dbt2looker --tag prod
```

**Generate Looker view files for all exposed models **
[dbt docs - exposures](https://docs.getdbt.com/docs/build/exposures)

```shell
dbt2looker --exposed_only
```

**Generate Looker view files with hidden=yes paramenter for all models**

```shell
dbt2looker --hidden_dimensions
```

## Install

**Install from PyPi repository**

Install from pypi into a fresh virtual environment.

```
# Create virtual env
python3.7 -m venv dbt2looker-venv
source dbt2looker-venv/bin/activate

# Install
pip install dbt2looker-bigquery

# Run
dbt2looker
```

Or if you use uv

```
# Create virtual env
uv add dbt2looker-bigquery

# Run
dbt2looker
```

## Defining measures

You can define looker measures in your dbt `schema.yml` files. For example:

```yaml
models:
  - name: pages
    columns:
      - name: url
        description: "Page url"
      - name: event_id
        description: unique event id for page view
        meta:
          looker:
            hidden: True
            label: event
            group_label: identifiers
            value_format_name: id

          looker_measures:
            - type: count_distinct
              sql_distinct_key: ${url}
            - type: count
              value_format_name: decimal_1
```
