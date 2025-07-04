import os
import yaml
from databricks import sql

def catalog_exists(cursor, catalog_name: str) -> bool:
    cursor.execute("SHOW CATALOGS")
    return any(catalog_name == row[0] for row in cursor.fetchall())

def main():
    # --- Step 1: Determine environment and branch name ---
    branch = os.getenv("GITHUB_REF_NAME", "dev")
    environment = os.getenv("ENVIRONMENT", "dev")

    if branch not in ["main", "dev", "test"]:
        raise ValueError(f"Unsupported branch: {branch}")
    print(f"Provisioning for environment: {environment}")

    # --- Step 2: Load environment configuration ---
    with open("environments.yaml") as f:
        env_config = yaml.safe_load(f)

    env_settings = env_config["environments"][environment]
    databricks_host = env_settings["databricks_host"]
    databricks_endpoint = env_settings["databricks_endpoint"]
    metastore_type = env_settings.get("metastore_type", "unity")  # default: unity

    databricks_token = os.getenv("DATABRICKS_TOKEN")
    if not databricks_token:
        raise EnvironmentError("DATABRICKS_TOKEN not set")

    # --- Step 3: Load dataproduct descriptor ---
    with open("dataproduct.yaml") as f:
        descriptor = yaml.safe_load(f)

    platform = descriptor["platform"]
    domain = descriptor["domain"]
    name = descriptor["name"]
    description = descriptor["description"]

    properties = {
        "dataproduct_owner": descriptor["owner"],
        "dataproduct_name": name,
        "platform": platform,
        "domain": domain
    }
    dbprops = ", ".join(f"{key} = '{value}'" for key, value in properties.items())

    print(f"Connecting to Databricks ({metastore_type} metastore)...")

    sql_client = sql.connect(
        server_hostname=databricks_host,
        http_path=databricks_endpoint,
        access_token=databricks_token,
    )
    cursor = sql_client.cursor()

    if metastore_type == "unity":
        # --- Unity Catalog ---
        catalog_name = f"{platform}_{domain}_{environment}"
        schema_name = f"{catalog_name}.{name}".replace('-', '_')

        if not catalog_exists(cursor, catalog_name):
            print(f"⚠️ Catalog '{catalog_name}' does not exist. Creating...")
            cursor.execute(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
            print(f"✅ Catalog '{catalog_name}' created.")

    elif metastore_type == "hive":
        # --- Hive Metastore ---
        schema_name = f"{platform}_{domain}_{name}".replace('-', '_')

    else:
        raise ValueError(f"Unsupported metastore_type: {metastore_type}")

    # --- Create schema ---
    print(f"Creating schema: {schema_name}")
    cursor.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name}
        COMMENT '{description}'
        WITH DBPROPERTIES ({dbprops})
        """
    )

    cursor.close()
    sql_client.close()
    print("✅ Schema provisioning completed.")

if __name__ == "__main__":
    main()