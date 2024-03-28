from res.utils.logging import logger
import sqlalchemy as sa
from res.connectors.postgres.utils import (
    get_tables,
    get_columns,
    build_insert_columns,
    build_insert_values,
)


class PostgresSync:
    def __init__(
        self,
        source_url: str,
        target_url: str,
        yes_i_want_to_write_to_target: bool = False,
    ):
        """
        Creates a new PostgresSync instance. Includes a check to prevent accidentally
        writing to a primary database.

        Args:
            source_url: The URL of the source Hasura database.
            target_url: The URL of the target Hasura database.

        Example:
            PostgresSync(SOURCE_HASURA_URL, TARGET_HASURA_URL).sync({
                'meta': {
                    'bodies': '*',
                    'materials': '*'
                },
            })
        """

        if "res_primary" in target_url and not yes_i_want_to_write_to_target:
            raise ValueError(
                "You are trying to sync to a primary database. Please use the yes_i_want_to_write_to_target flag if you really want to do this."
            )

        logger.info(f"SQLAlchemy connecting...")
        self.source_engine = sa.create_engine(source_url)
        self.target_engine = sa.create_engine(target_url)

    def sync(self, query: dict[str, str | dict[str, int | str]]):
        """
        Syncs the target database with the source database using the provided query,
        Before running a sync operation, ensure the tables are ordered by foreign key
        dependencies. For example, if you have a table `body_pieces` that has a foreign key
        to `bodies`, you should sync `bodies` first.

        Args:
            query: A dictionary of schemas and tables to sync.

        Example:
            postgres_sync.sync({
                'meta': {
                    'bodies': '*', # sync all rows from bodies
                    'body_pieces': 100 # sync only 100 rows from body_pieces
                },
                'make': '*', # sync all tables from make
            })

        """
        try:
            if query == "*":
                raise ValueError("* not supported at root. Schemas must be specified")

            target_conn = self.target_engine.connect()
            source_conn = self.source_engine.connect()

            for schema, tables in query.items():
                if tables == "*":
                    tables = {t: "*" for t in get_tables(target_conn, schema)}

                for table, rows in tables.items():
                    logger.info(f"Syncing {schema}.{table}...")

                    local_columns = sorted(get_columns(target_conn, schema, table))
                    remote_columns = sorted(get_columns(source_conn, schema, table))
                    if local_columns != remote_columns:
                        print(f"Columns for {schema}.{table} are not the same")
                        print(f"Local: {local_columns}")
                        print(f"Remote: {remote_columns}")
                        print("")
                        raise ValueError(
                            f"Columns for {schema}.{table} do not match remote"
                        )
                    logger.info(f"Fetching {schema}.{table}...")
                    if rows == "*":
                        query = f"SELECT * FROM {schema}.{table}"
                    elif isinstance(rows, int):
                        query = f"SELECT * FROM {schema}.{table} LIMIT {rows}"
                    else:
                        raise ValueError(f"Invalid rows value: {rows}")

                    remote_data = source_conn.execute(sa.text(query)).fetchall()

                    logger.info(
                        f"Inserting {len(remote_data)} into {schema}.{table}..."
                    )

                    to_insert = [row._asdict() for row in remote_data]

                    value_names = build_insert_columns(to_insert)
                    value_items = build_insert_values(to_insert)

                    # Empty the target table first while reseting the auto increment
                    target_conn.execute(
                        sa.text(
                            f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE;"
                        )
                    )

                    # Insert the values
                    target_conn.execute(
                        sa.text(
                            f"INSERT INTO {schema}.{table} {value_names} VALUES {value_items}"
                        )
                    )

        finally:
            target_conn.close()
            source_conn.close()
