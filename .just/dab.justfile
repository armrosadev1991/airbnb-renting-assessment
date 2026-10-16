# Loads the PROFILE_NAME for the Databricks CLI from your environment, or defaults to "DEFAULT" if not set.
PROFILE_NAME := env("PROFILE_NAME", "DEFAULT")

# The default target for all databricks bundle commands, defaults to "dev" if not set.
DEFAULT_TARGET := env("DEFAULT_TARGET", "dev")

# Shared landing zone (catalog / schema / volume) that every bundle target reads from.
LANDING_CATALOG := env("LANDING_CATALOG", "airbnb_renting")
SQL_WAREHOUSE := env("SQL_WAREHOUSE", "Serverless Starter Warehouse")

# Deliberately NOT a bundle resource: `bundle destroy` would otherwise delete the raw files
# that were uploaded by hand. Uses SQL because on Free Edition only SQL `CREATE CATALOG`
# picks up Default Storage; `IF NOT EXISTS` makes every statement idempotent.
# Create the shared landing zone (catalog, landing schema, raw volume); safe to re-run
[group('dab')]
[script]
bootstrap catalog=LANDING_CATALOG:
	set -euo pipefail;
	sql() { ./.just/run_sql.sh "{{ PROFILE_NAME }}" "{{ SQL_WAREHOUSE }}" "$1"; };
	echo "Creating catalog, schema and volume (starts the SQL warehouse if stopped)...";
	sql "CREATE CATALOG IF NOT EXISTS {{ catalog }} COMMENT 'Airbnb vs Kamernet assessment'";
	sql "CREATE SCHEMA IF NOT EXISTS {{ catalog }}.landing COMMENT 'Raw files uploaded by hand'";
	sql "CREATE VOLUME IF NOT EXISTS {{ catalog }}.landing.raw COMMENT 'One folder per source'";
	# Auto Loader reads a directory per source, so each file gets its own folder.
	for d in airbnb rentals geo; do
		databricks fs mkdir "dbfs:/Volumes/{{ catalog }}/landing/raw/$d" --profile "{{ PROFILE_NAME }}";
	done;
	echo "✅  Upload files to /Volumes/{{ catalog }}/landing/raw/{airbnb,rentals,geo}/";

# Validate Databricks bundle configuration and resources, targets development environment by default
[group('dab')]
validate target=DEFAULT_TARGET:
	uv sync;
	uv build >/dev/null;
	echo "Validating resources...";
	databricks bundle validate --profile {{ PROFILE_NAME }} --target {{ target }};

# Provides a list of actions that would be performed against a target when the bundle would be deployed.
[group('dab')]
plan target=DEFAULT_TARGET:
	databricks bundle plan --profile {{ PROFILE_NAME }} --target {{ target }};

# Deploy the Databricks bundle, targets development environment by default
[group('dab')]
deploy target=DEFAULT_TARGET:
	uv sync;
	uv build >/dev/null;
	echo "Deploying resources...";
	databricks bundle deploy --profile {{ PROFILE_NAME }} --target {{ target }};

# Destroy all deployed Databricks resources against target environment, targets development environment by default
[group('dab')]
[script]
destroy target=DEFAULT_TARGET:
	echo "Destroying resources...";
	databricks bundle destroy --profile {{ PROFILE_NAME }} --target {{ target }};

# Provide overview of the currently deployed resources.
[group('dab')]
summary target=DEFAULT_TARGET:
	echo "Deployed resources:";
	databricks bundle summary --profile {{ PROFILE_NAME }} --target {{ target }};
alias bundle-info := summary

# Open a resource in the browser, you will be prompted to select the desired resource.
[group('dab')]
[script]
open target=DEFAULT_TARGET:
	databricks bundle open --profile {{ PROFILE_NAME }} --target {{ target }};
alias go-to-bundle := open
alias bundle-open := open

# Run the full workflow (pipeline refresh, then parquet export), targets dev by default
[group('dab')]
run target=DEFAULT_TARGET:
	databricks bundle run airbnb_renting_job --profile {{ PROFILE_NAME }} --target {{ target }};

# Copy the gold parquet exports from the target's volume into ./data/output
[group('dab')]
[script]
export target=DEFAULT_TARGET:
	set -euo pipefail;
	gold=$(databricks bundle validate --profile {{ PROFILE_NAME }} --target {{ target }} -o json \
		| python3 -I -c 'import json,sys; r=json.load(sys.stdin)["resources"]; print(r["schemas"]["gold"]["name"])');
	mkdir -p data/output;
	databricks fs cp -r "dbfs:/Volumes/{{ LANDING_CATALOG }}/$gold/exports" data/output --overwrite --profile {{ PROFILE_NAME }};
	ls -la data/output;
