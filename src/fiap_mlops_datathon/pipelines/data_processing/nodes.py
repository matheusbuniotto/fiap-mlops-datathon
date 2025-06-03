"""
Nodes for SQL to Parquet processing.
"""

import duckdb
import pandas as pd
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

def process_sql_to_parquet(sql_query: str, bronze_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Process SQL query and return DataFrame.

    Args:
        sql_query: SQL query to execute
        bronze_data: Optional DataFrame for queries that reference existing data

    Returns:
        Processed DataFrame
    """
    con = None
    try:
        # Create a connection and register bronze data if provided
        con = duckdb.connect(database=':memory:')
        if bronze_data is not None:
            con.register('bronze_data', bronze_data)

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

def process_applicants_silver(sql_query: str, bronze_applicants: pd.DataFrame) -> pd.DataFrame:
    """Process applicants data to silver layer."""
    return process_sql_to_parquet(sql_query, bronze_applicants)

def process_vagas_silver(sql_query: str, bronze_vagas: pd.DataFrame) -> pd.DataFrame:
    """Process job positions data to silver layer."""
    return process_sql_to_parquet(sql_query, bronze_vagas)

def process_prospects_silver(sql_query: str, bronze_prospects: pd.DataFrame) -> pd.DataFrame:
    """Process prospects data to silver layer."""
    return process_sql_to_parquet(sql_query, bronze_prospects)

def process_silver_tables(
    applicants_sql: str,
    prospects_sql: str,
    vagas_sql: str,
    bronze_applicants: pd.DataFrame,
    bronze_prospects: pd.DataFrame,
    bronze_vagas: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """
    Process all individual tables to silver layer in a single node.

    Args:
        applicants_sql: SQL query for applicants
        prospects_sql: SQL query for prospects
        vagas_sql: SQL query for job positions
        bronze_applicants: Bronze applicants DataFrame
        bronze_prospects: Bronze prospects DataFrame
        bronze_vagas: Bronze job positions DataFrame

    Returns:
        Dictionary containing processed DataFrames
    """
    logger.info("Processing all tables to silver layer...")
    
    # Process each table
    silver_applicants = process_sql_to_parquet(applicants_sql, bronze_applicants)
    logger.info(f"Processed applicants: {len(silver_applicants)} rows")
    
    silver_prospects = process_sql_to_parquet(prospects_sql, bronze_prospects)
    logger.info(f"Processed prospects: {len(silver_prospects)} rows")
    
    silver_vagas = process_sql_to_parquet(vagas_sql, bronze_vagas)
    logger.info(f"Processed job positions: {len(silver_vagas)} rows")
    
    return {
        "silver_applicants": silver_applicants,
        "silver_prospects": silver_prospects,
        "silver_vagas": silver_vagas
    }

def process_core_silver(
    sql_query: str,
    silver_applicants: pd.DataFrame,
    silver_vagas: pd.DataFrame,
    silver_prospects: pd.DataFrame
) -> pd.DataFrame:
    """Process core integrated data to silver layer."""
    con = None
    try:
        con = duckdb.connect(database=':memory:')
        
        # Register all necessary tables
        con.register('silver_applicants', silver_applicants)
        con.register('silver_vagas', silver_vagas)
        con.register('silver_prospects', silver_prospects)

        # Execute the query
        df = con.execute(sql_query).df()
        logger.info(f"Successfully processed core SQL query, resulting in {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Error processing core SQL: {str(e)}")
        raise
    finally:
        if con is not None:
            con.close()
