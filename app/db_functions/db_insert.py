import os
import psycopg2
import pandas as pd
import json
from dotenv import load_dotenv
from psycopg2 import sql

class DBUpdate_Postgres:
    def __init__(self):
        load_dotenv()

        self.db_name = os.getenv("PG_DB_NAME")
        self.db_user = os.getenv("PG_DB_USER")
        self.db_password = os.getenv("PG_DB_PASSWORD")
        self.db_host = os.getenv("PG_DB_HOST", "localhost")
        self.db_port = os.getenv("PG_DB_PORT", "5432")
        self.schema_name = os.getenv("PG_SCHEMA_NAME")
        self.table_name = os.getenv("PG_TABLE_NAME")
        self.data_file = os.getenv("DATA_FILE_PATH")
        self.data_format = os.getenv("DATA_FILE_FORMAT", "csv").lower()

        for var in [self.db_name, self.db_user, self.db_password, self.schema_name, self.table_name, self.data_file]:
            if not var:
                raise ValueError("Missing required environment variable.")

    def create_database_if_not_exists(self):
        try:
            conn = psycopg2.connect(dbname="postgres", user=self.db_user, password=self.db_password,
                                    host=self.db_host, port=self.db_port)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.db_name,))
            if not cur.fetchone():
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name)))
        finally:
            cur.close()
            conn.close()

    def connect_to_db(self):
        return psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_password,
                                host=self.db_host, port=self.db_port)

    def load_data(self):
        if self.data_format == "csv":
            return pd.read_csv(self.data_file)
        elif self.data_format == "json":
            with open(self.data_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported data format: {self.data_format}")

    def create_schema_and_table(self, cur, df):
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema_name)))
        columns = [sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in df.columns]
        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(self.schema_name),
            sql.Identifier(self.table_name),
            sql.SQL(", ").join(columns)
        )
        cur.execute(create_table_query)

    def insert_data(self, cur, df):
        for _, row in df.iterrows():
            values = [str(val) if pd.notnull(val) else None for val in row]
            insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(self.schema_name),
                sql.Identifier(self.table_name),
                sql.SQL(", ").join(map(sql.Identifier, df.columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(values))
            )
            cur.execute(insert_query, values)

    def run(self):
        self.create_database_if_not_exists()
        conn = None
        cur = None
        try:
            conn = self.connect_to_db()
            cur = conn.cursor()
            df = self.load_data()

            self.create_schema_and_table(cur, df)
            self.insert_data(cur, df)

            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
          
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

# Run the loader
if __name__ == "__main__":
    loader = DBUpdate_Postgres()
    loader.run()



# PG_DB_NAME=mydb
# PG_DB_USER=postgres
# PG_DB_PASSWORD=securepassword
# PG_DB_HOST=localhost
# PG_DB_PORT=5432
# PG_SCHEMA_NAME=myschema
# PG_TABLE_NAME=mytable
# DATA_FILE_PATH=data.json
# DATA_FILE_FORMAT=json