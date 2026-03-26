DAGSTER_HOME_PRD=~/OneDrive/dagster

prd:
	DAGSTER_HOME=${DAGSTER_HOME_PRD} ENVIRONMENT=prd uv run dg dev

dev:
	uv run dg dev