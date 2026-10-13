from datetime import datetime

import psycopg2.extensions
from prefect import get_run_logger
from prefect_sqlalchemy import DatabaseCredentials
from psycopg2 import sql

Connection = psycopg2.extensions.connection

def truncate_table(conn: Connection, table_name: str):
    get_run_logger().info(f"Truncating table: {table_name}")
    with conn, conn.cursor() as curs:
        curs.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name)))


def create_teamleader_resource_table(table_name: str, conn: Connection):
    get_run_logger().info(f"Ensuring resource table: {table_name} exists")
    with conn, conn.cursor() as curs:
        curs.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {table} (
                    id serial PRIMARY KEY,
                    tl_uuid uuid NOT NULL,
                    tl_content jsonb NOT NULL,
                    tl_type VARCHAR,
                    created_at timestamp with time zone NOT NULL DEFAULT now(),
                    updated_at timestamp with time zone NOT NULL DEFAULT now(),
                    CONSTRAINT {constraint} UNIQUE (tl_uuid)
                );
                """
            ).format(
                table=sql.Identifier(table_name),
                constraint=sql.Identifier(f"{table_name}_constraint_key"),
            )
        )


def upsert_into_table(conn: Connection, table_name: str, data: list[tuple]):
    logger = get_run_logger()
    if not data:
        logger.info(f"No data to upsert into {table_name}.")
        return
    
    logger.info(f"Upserting {len(data)} rows into table: {table_name}")
    with conn, conn.cursor() as curs:
        curs.executemany(
            sql.SQL("""INSERT INTO {} (
                            tl_uuid,
                            tl_type,
                            tl_content
                        )
                        VALUES (%s, %s, %s) ON CONFLICT (tl_uuid) DO
                        UPDATE
                        SET tl_content = EXCLUDED.tl_content,
                            tl_type = EXCLUDED.tl_type,
                            updated_at = now();
                        """).format(sql.Identifier(table_name)),
            data,
        )


def connect_database(db_block_name: str) -> Connection:
    get_run_logger().info("Creating database connection")
    postgres_credentials: DatabaseCredentials = DatabaseCredentials.load(db_block_name)
    password = (
        postgres_credentials.password.get_secret_value()
        if postgres_credentials.password is not None
        else None
    )
    return psycopg2.connect(
        user=postgres_credentials.username,
        password=password,
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
    )


def get_last_modified_date(conn: Connection, table_name: str) -> datetime:
    get_run_logger().info(f"Fetching last modified date from table: {table_name}")
    with conn, conn.cursor() as curs:
        curs.execute(
            sql.SQL("SELECT max(updated_at) FROM {}").format(sql.Identifier(table_name))
        )
        result_list = curs.fetchone()
        if result_list is None:
            raise ValueError("Could not fetch last updated date")
        return result_list[0]

