import logging
import os
from string import Template
import yaml
from Extractor.database_connector import DatabaseConnector
from sqlalchemy import text
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_PATH = "Extractor/config.yaml"

load_dotenv()


def load_config(config_path):
    with open(config_path, "r") as file:
        config_content = file.read()
    template = Template(config_content)
    config_content = template.safe_substitute(os.environ)
    return yaml.safe_load(config_content)


def get_table_columns(engine, table_name, schema):
    with engine.connect() as conn:
        result = conn.execute(
            text(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name.lower()}'
                ORDER BY ordinal_position
                """
            )
        )
        return [row[0] for row in result]


def archive_table(engine, source_table, archive_table, source_schema="landing", archive_schema="archive"):
    source_columns = get_table_columns(engine, source_table, source_schema)
    archive_columns = get_table_columns(engine, archive_table, archive_schema)

    common_columns = [col for col in source_columns if col in archive_columns]
    columns_str = ", ".join(common_columns)
    insert_columns = columns_str + ", archived_at"
    select_columns = columns_str + ", :archived_at"
    params = {"archived_at": datetime.now()}

    sql = text(
        f"""
        INSERT INTO {archive_schema}.{archive_table} ({insert_columns})
        SELECT {select_columns} FROM {source_schema}.{source_table}
        """
    )

    with engine.connect() as conn:
        result = conn.execute(sql, params)
        conn.commit()
        logger.info(f"Archived {result.rowcount if hasattr(result, 'rowcount') else 'data'} rows from {source_schema}.{source_table} to {archive_schema}.{archive_table}")


def main():
    config = load_config(CONFIG_PATH)
    db_connector = DatabaseConnector(config)
    engine = db_connector.get_engine()

    landing_tables = set()

    for table in config.get("s3", {}).get("files", {}).values():
        landing_tables.add(table)

    for table in config.get("api", {}).get("endpoints", {}).values():
        landing_tables.add(table)

    for table in landing_tables:
        archive_table_name = f"archive_{table}"
        try:
            archive_table(engine, table, archive_table_name, source_schema="landing", archive_schema="archive")
        except Exception as e:
            logger.error(f"Failed to archive {table}: {str(e)}")

    engine.dispose()


if __name__ == "__main__":
    main()
