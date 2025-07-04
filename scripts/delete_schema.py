import os
import yaml
from databricks import sql

def catalog_exists(cursor, catalog_name: str) -> bool:
    cursor.execute("SHOW CATALOGS")
    return any(catalog_name == row[0] for row in cursor.fetchall())

def schema_exists(cursor, catalog: str, schema: str, metastore_type: str) -> bool:
    if metastore_type == "unity":
        cursor.execute(f"SHOW SCHEMAS IN {catalog}")
    else:  # hive non ha catalog
        cursor.execute("SHOW SCHEMAS")
    return any(schema == row[0] for row in cursor.fetchall())

def main():
    # --- Step 1: Determine environment and branch name ---
    branch = os.getenv("GITHUB_REF_NAME", "dev")
    environment = os.getenv("ENVIRONMENT", "dev")

    if branch not in ["main", "dev", "test"]:
        raise ValueError(f"Unsupported branch: {branch}")
    print(f"Deleting schema for environment: {environment}")

    # --- Step 2: Load environment configuration ---
    with open("environments.yaml") as f:
        env_config = yaml.safe_load(f)

    env_settings = env_config["environments"][environment]
    databricks_host = env_settings["databricks_host"]
    databricks_endpoint = env_settings["databricks_endpoint"]
    metastore_type = env_settings.get("metastore_type", "unity")

    databricks_token = os.getenv("DATABRICKS_TOKEN")
    if not databricks_token:
        raise EnvironmentError("DATABRICKS_TOKEN not set")

    # --- Step 3: Load dataproduct descriptor ---
    with open("dataproduct.yaml") as f:
        descriptor = yaml.safe_load(f)

    platform = descriptor["platform"]
    domain = descriptor["domain"]
    name = descriptor["name"]

    sql_client = sql.connect(
        server_hostname=databricks_host,
        http_path=databricks_endpoint,
        access_token=databricks_token,
    )
    cursor = sql_client.cursor()

    if metastore_type == "unity":
        catalog = f"{platform}_{domain}_{environment}"
        schema = name.replace('-', '_')
        full_schema = f"{catalog}.{schema}"

        if not catalog_exists(cursor, catalog):
            print(f"⚠️ Catalog '{catalog}' does not exist. Nothing to delete.")
        elif not schema_exists(cursor, catalog, schema, metastore_type):
            print(f"⚠️ Schema '{full_schema}' does not exist. Nothing to delete.")
        else:
            print(f"🗑️ Deleting schema: {full_schema}...")
            cursor.execute(f"DROP SCHEMA IF EXISTS {full_schema} CASCADE")
            print(f"✅ Schema '{full_schema}' deleted.")

    elif metastore_type == "hive":
        schema = f"{platform}_{domain}_{name}".replace('-', '_')

        if not schema_exists(cursor, "", schema, metastore_type):
            print(f"⚠️ Schema '{schema}' does not exist. Nothing to delete.")
        else:
            print(f"🗑️ Deleting schema: {schema}...")
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            print(f"✅ Schema '{schema}' deleted.")
    else:
        raise ValueError(f"Unsupported metastore_type: {metastore_type}")

    cursor.close()
    sql_client.close()

if __name__ == "__main__":
    main()