# How to publish a new version to Pypi
poetry bump
poetry build
twine upload dist/* --repository dbt2looker-bigquery