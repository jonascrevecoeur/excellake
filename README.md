# Excellake

## Overview

Excellake is a data architecture for small companies which want to provide reliable and fresh data to business users for reporting in Excel.

More information on the architecture choices leading to Excellake can be found in this medium article.

## Getting started

### Configuration developer laptop

**Prerequisites:**

- Mounted storage location
- Git
- Python

**Installation:**

```bash
# Clone the repository
git clone https://github.com/jonascrevecoeur/excellake
cd excellake

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Activate the virtual environment
source .venv/bin/activate
```

**Configure environment variables**

Create a file `.env` as 

```
# Directory for storing data for local development
DATA_DIRECTORY_DEV=
# Data directory in the shared drive 
# Used when inspecting and fixing issues in production
DATA_DIRECTORY_PRD=~/OneDrive/data
```

Update line 1 in `makefile` to point to a folder in the shared drive for storing metadata in production runs

```
DAGSTER_HOME_PRD=~/OneDrive/dagster
```

### Configuration pipeline runner

This section configures the computer that will run the production data pipelines.

**GitHub runner setup**

- Create a private fork of `jonascrevecoeur/excellake`
- Configure a GitHub self-hosted runner on the computre running production pipelines following [this guide](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/add-runners#adding-a-self-hosted-runner-to-a-repository)
- Define the following environment variables on the Git repo
  - ENVIRONMENT: prd
  - DATA_DIRECTORY_PRD: (location to store data)
  - DAGSTER_HOME: (location to store run metadata)
- Update line __ in `.github/workflows/daily_job.yml` to use your GitHub self-hosted runner

## Local development

- Add new assets in `src/excellake/assets`
- Test materialization of a single asset using 
  ```
  uv run dg launch --asset <asset name>
  ```
- Check asset dependencies in the ui via
  ```
  make dev
  ```
- Interact and check on production data via 
  ```
  make prd
  ```

When your changes are ready commit them to master to let the data pipeline refresh your data once a day.