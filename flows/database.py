from datetime import datetime

import psycopg2.extensions
from prefect import get_run_logger, task
from prefect_sqlalchemy import DatabaseCredentials
from pydantic import SecretStr

from .models import Harvest_Tables, Resource, TL_Auth, TL_Client

Connection = psycopg2.extensions.connection

@task
def truncate_table(conn: Connection, table: str):
    get_run_logger().info(f"Truncating table: {table}")
    with conn, conn.cursor() as curs:
        curs.execute(f"TRUNCATE TABLE {table}")


@task
def create_teamleader_auth_table(conn: Connection):
    get_run_logger().info(f"Ensuring auth table: {Harvest_Tables.tl_oauth} exists")
    with conn, conn.cursor() as curs:
        curs.execute(
            f"""
                CREATE TABLE IF NOT EXISTS {Harvest_Tables.tl_oauth} (
                    id serial PRIMARY KEY,
                    code VARCHAR,
                    auth_token VARCHAR,
                    refresh_token VARCHAR,
                    created_at timestamp with time zone NOT NULL DEFAULT now(),
                    updated_at timestamp with time zone NOT NULL DEFAULT now()
                );
                """
        )


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
def get_auth_tokens_from_db(
    conn: Connection, tl_auth_uri: str, tl_client: TL_Client
) -> TL_Auth:
    get_run_logger().info("Fetching Teamleader tokens from database")
    with conn, conn.cursor() as curs:
        curs.execute(f"SELECT * FROM {Harvest_Tables.tl_oauth} LIMIT 1;")
        result = curs.fetchone()
    if result is None:
        raise KeyError(
            f"Missing authorization data in database table {Harvest_Tables.tl_oauth}"
        )
    return TL_Auth(
        uri=tl_auth_uri,
        client_id=tl_client.client_id,
        client_secret=tl_client.client_secret,
        refresh_token=SecretStr(result[3]),
        access_token=SecretStr(result[2]),
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


@task
def validate_db_auth_state(conn: Connection):
    get_run_logger().info("Validating database auth state")
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {Harvest_Tables.tl_oauth}")
        count_fetch = cursor.fetchone()

    if count_fetch is None:
        raise RuntimeError("Error on fetch of tl_oauth")

    count = count_fetch[0]
    if count != 1:
        raise ValueError("There should be exactly 1 row in tl_oauth")
