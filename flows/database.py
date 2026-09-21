from datetime import datetime

import psycopg2.extensions
from prefect import get_run_logger, task
from prefect_sqlalchemy import DatabaseCredentials

from .models import Resource

Connection = psycopg2.extensions.connection

@task
def truncate_table(conn: Connection, table: str):
    get_run_logger().info(f"Truncating table: {table}")
    with conn, conn.cursor() as curs:
        curs.execute(f"TRUNCATE TABLE {table}")


@task
def create_teamleader_resource_table(resource: Resource, conn: Connection):
    get_run_logger().info(f"Ensuring resource table: {resource} exists")
    table_name = Resource.get_db_table_name(resource)
    with conn, conn.cursor() as curs:
        curs.execute(
            f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id serial PRIMARY KEY,
                    tl_uuid uuid NOT NULL,
                    tl_content jsonb NOT NULL,
                    tl_type VARCHAR,
                    created_at timestamp with time zone NOT NULL DEFAULT now(),
                    updated_at timestamp with time zone NOT NULL DEFAULT now(),
                    CONSTRAINT {table_name}_constraint_key UNIQUE (tl_uuid)
                );
                """
        )


@task
def upsert_into_table(conn: Connection, table: str, data: list[tuple]):
    logger = get_run_logger()
    if not data:
        logger.info(f"No data to upsert into {table}.")
        return
    
    logger.info(f"Upserting {len(data)} rows into table: {table}")
    with conn, conn.cursor() as curs:
        curs.executemany(
            f"""INSERT INTO {table} (
                            tl_uuid,
                            tl_type,
                            tl_content
                        )
                        VALUES (%s, %s, %s) ON CONFLICT (tl_uuid) DO
                        UPDATE
                        SET tl_content = EXCLUDED.tl_content,
                            tl_type = EXCLUDED.tl_type,
                            updated_at = now();
                        """,
            data,
        )


@task
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


@task
def get_last_modified_date(conn: Connection, table: str) -> datetime:
    get_run_logger().info(f"Fetching last modified date from table: {table}")
    with conn, conn.cursor() as curs:
        curs.execute(f"SELECT max(updated_at) FROM {table}")
        result_list = curs.fetchone()
        if result_list is None:
            raise ValueError("Could not fetch last updated date")
        return result_list[0]

