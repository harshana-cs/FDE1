from psycopg2.extras import Json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JSONExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def load_to_landing(self, table_name, json_data, source_info):
      conn = self.db_connector.get_connection()
      cursor = None
      try:
          cursor = conn.cursor()

          # Check if 'file_name' exists in the table's columns
          cursor.execute("""
              SELECT column_name
              FROM information_schema.columns
              WHERE table_schema = 'Landing' AND table_name = %s
          """, (table_name,))
          columns = [row[0] for row in cursor.fetchall()]
          has_file_name = 'file_name' in columns

          # Prepare dynamic insert statement
          if has_file_name:
              insert_stmt = f"""
                  INSERT INTO Landing.{table_name} (raw_data, file_name)
                  VALUES (%s, %s)
              """
          else:
              insert_stmt = f"""
                  INSERT INTO Landing.{table_name} (raw_data)
                  VALUES (%s)
              """

          # Insert data
          if isinstance(json_data, list):
              for item in json_data:
                  if has_file_name:
                      cursor.execute(insert_stmt, (Json(item), source_info))
                  else:
                      cursor.execute(insert_stmt, (Json(item),))
          else:
              if has_file_name:
                  cursor.execute(insert_stmt, (Json(json_data), source_info))
              else:
                  cursor.execute(insert_stmt, (Json(json_data),))

          conn.commit()
          logger.info(f"Loaded JSON data to {table_name}")

      except Exception as e:
          conn.rollback()
          logger.error(f"Error loading to {table_name}: {str(e)}")
          raise
      finally:
          if cursor:
              cursor.close()
          conn.close()
          
          
          
            
            
            
            
            
            
            
            
            
            
            
            
            
            