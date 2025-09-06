import logging
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    @staticmethod
    def camel_to_snake(name):
        """
        Convert camelCase or PascalCase to snake_case.
        Example: 'customerKey' -> 'customer_key', 'StoreID' -> 'store_id'
        """
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def load_to_landing(self, table_name, df, source_info, main_extractor=None):
        table_name = table_name.lower()
        table_columns = main_extractor.get_table_columns(table_name, schema="landing")
        engine = self.db_connector.get_engine()
        try:

            def normalize_column(col):
                col = col.replace(" ", "").replace("-", "_")
                col = re.sub(r"_+", "_", col)
                return self.camel_to_snake(col)

            df.columns = [normalize_column(col) for col in df.columns]

            # Add metadata columns if they exist in the table schema
            if "loaded_at" in table_columns:
                df["loaded_at"] = datetime.now()
            if "source_file" in table_columns:
                df["source_file"] = source_info

            # Only keep columns that exist in the table schema
            existing_columns = [col for col in df.columns if col in table_columns]
            missing_columns = [col for col in df.columns if col not in table_columns]
            if missing_columns:
                logger.warning(
                    f"The following columns will be skipped (not in table {table_name}): {missing_columns}"
                )

            df_filtered = df[existing_columns]
            df_filtered.to_sql(
                table_name, engine, schema="landing", if_exists="append", index=False
            )
            logger.info(
                f"Successfully loaded {len(df_filtered)} rows into {table_name}"
            )
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {e}")
            raise
