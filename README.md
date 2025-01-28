# dbt2looker-bigquery (active as of 13.9.2024)

Use `dbt2looker-bigquery` to generate Looker view files automatically from dbt models in Bigquery.

# Acknowledgments

Higly inspired by dbt2lookml, all credit to @magnus-ffcg for the structure, he has refactored most of the code.

This is a fork of dbt2looker that is specific to bigquery.
Most of the code has been refactored by @
The intention is to allow one to define most of the simple and tedious lookml settings in dbt.
That way the lookml code gets less bloated, and can be more focused on advanced metrics and explores.

Want a deeper integration between dbt and your BI tool?
You should also checkout [Lightdash - the open source alternative to Looker](https://github.com/lightdash/lightdash)

**Features**

- Warehouses: BigQuery

- **Column descriptions** synced to looker
- **Define Dimension lookml** define common lookml settings in dbt like label, group label, hidden
- **Define Measures lookml** simple measures can be defined from dbt
- **Create explores for structs** automatically generate explores for complex tables with structs and arrays in them.
- **Dimension groups** for datetime/timestamp/date columns

## Quickstart

Run `dbt2looker` in the root of your dbt project _after compiling looker docs_.
(dbt2looker-bigquery uses docs to infer types and such)

**Generate Looker view files for all models**

```shell
dbt2looker
```

## Install

**Install from PyPi repository**

```
uv add dbt2looker-bigquery

# Run
dbt2looker
```

## cli args

```
options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --target-dir TARGET_DIR
                        Path to dbt target directory containing manifest.json and catalog.json. Default is "./target"
  --tag TAG             Filter to dbt models using this tag, can be combined with --exposures-only to only generate lookml files for exposures with this tag
  --log-level {DEBUG,INFO,WARN,ERROR}
                        Set level of logs. Default is INFO
  --output-dir OUTPUT_DIR
                        Path to a directory that will contain the generated lookml files
  --exposures-only      add this flag to only generate lookml files for exposures
  --exposures-tag EXPOSURES_TAG
                        filter to exposures with a specific tag
  --skip-explore        add this flag to skip generating an sample "explore" in views for nested structures
  --use-table-name      Experimental: add this flag to use table names on views and explore
  --select SELECT       select a specific model to generate lookml for, ignores tag and explore
  --generate-locale     Experimental: Generate locale files for each label on each field in view
  --all-hidden          add this flag to force all dimensions and measures to be hidden
  --folder-structure FOLDER_STRUCTURE
                        Define the source of the folder structure. Default is 'BIGQUERY_DATASET', other option is 'DBT_FOLDER'
  --remove-prefix-from-dataset REMOVE_PREFIX_FROM_DATASET
                        Experimental: Remove prefix from dataset name, only works with 'BIGQUERY_DATASET' folder structure
  --implicit-primary-key
```

## lookml in dbt

```yaml
models:
  - name: dim_pages_v0
    meta:
      looker:
        view:
          label: Pages
        explore:
          group_label: Websites
          description: This explore lets you explore page data!
    columns:
      - name: url
        description: "Page url"
        meta:
          looker:
            dimension:
              label: "Page url from website"
              group_label: "Urls"
              description: "A url for a webpage that you can inspect"
            measures:
              - type: count
                label: Number of urls
              - type: count_distinct
                label: Distinct number of urls
```
