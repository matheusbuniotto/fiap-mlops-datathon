"""
Nodes for SQL to Parquet processing.
"""

import logging
from typing import Dict, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def process_sql_to_parquet(
    sql_query: str, intermediate_data: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Process SQL query and return DataFrame.

    Args:
        sql_query: SQL query to execute
        intermediate_data: Optional DataFrame for queries that reference existing data

    Returns:
        Processed DataFrame
    """
    con = None
    try:
        # Create a connection and register intermediate data if provided
        con = duckdb.connect(database=":memory:")
        if intermediate_data is not None:
            con.register("intermediate_data", intermediate_data)

        # Execute the query
        df = con.execute(sql_query).df()
        logger.info(f"Successfully processed SQL query, resulting in {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Error processing SQL: {str(e)}")
        raise
    finally:
        if con is not None:
            con.close()


def process_vagas_primary(
    sql_query: str, intermediate_vagas: pd.DataFrame
) -> pd.DataFrame:
    """Process job positions data to primary layer."""
    return process_sql_to_parquet(sql_query, intermediate_vagas)


def process_prospects_primary(
    sql_query: str, intermediate_prospects: pd.DataFrame
) -> pd.DataFrame:
    """Process prospects data to primary layer."""
    return process_sql_to_parquet(sql_query, intermediate_prospects)


def process_primary_tables(
    prospects_sql: str,
    vagas_sql: str,
    intermediate_prospects: pd.DataFrame,
    intermediate_vagas: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """
    Process all individual tables to primary layer in a single node.

    Args:
        prospects_sql: SQL query for prospects
        vagas_sql: SQL query for job positions
        intermediate_prospects: Intermediate prospects DataFrame
        intermediate_vagas: Intermediate job positions DataFrame

    Returns:
        Dictionary containing processed DataFrames
    """
    logger.info("Processing all tables to primary layer...")

    primary_prospects = process_sql_to_parquet(prospects_sql, intermediate_prospects)
    logger.info(f"Processed prospects: {len(primary_prospects)} rows")

    primary_vagas = process_sql_to_parquet(vagas_sql, intermediate_vagas)
    logger.info(f"Processed job positions: {len(primary_vagas)} rows")

    return {"primary_prospects": primary_prospects, "primary_vagas": primary_vagas}
