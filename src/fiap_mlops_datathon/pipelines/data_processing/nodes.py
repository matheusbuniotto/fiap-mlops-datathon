"""
Nodes for SQL to Parquet processing.
"""

import duckdb
import pandas as pd
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.feature_extraction.text import TfidfVectorizer

def process_sql_to_parquet(sql_query: str, intermediate_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Processa dados com tratamento de nulos, codificação categórica e embeddings de texto"""
    logger.info("Iniciando processamento de features")
    
    # Tratamento de valores nulos
    imputer = SimpleImputer(strategy='most_frequent')
    intermediate_data = pd.DataFrame(
        imputer.fit_transform(intermediate_data),
        columns=intermediate_data.columns
    )
    
    # Codificação de categorias ordenadas
    ordinal_encoder = OrdinalEncoder()
    categorical_cols = [col for col in intermediate_data.columns if 'categoria' in col]
    if categorical_cols:
        intermediate_data[categorical_cols] = ordinal_encoder.fit_transform(intermediate_data[categorical_cols])
    
    # Processamento de texto para colunas CV
    cv_cols = [col for col in intermediate_data.columns if col.startswith('cv_')]
    for col in cv_cols:
        tfidf = TfidfVectorizer(max_features=100)
        embeddings = tfidf.fit_transform(intermediate_data[col])
        intermediate_data[f'{col}_embedding'] = list(embeddings.toarray())
        logger.info(f"Texto processado para {col} com {len(tfidf.vocabulary_)} termos")
    
    return intermediate_data

def split_train_test(data: pd.DataFrame, test_size: float = 0.2) -> tuple:
    """Divide os dados em conjuntos de treino e teste"""
    from sklearn.model_selection import train_test_split
    logger.info(f"Dividindo dados em treino/teste (test_size={test_size})")
    return train_test_split(data, test_size=test_size, random_state=42)
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
        con = duckdb.connect(database=':memory:')
        if intermediate_data is not None:
            con.register('intermediate_data', intermediate_data)

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

def process_applicants_primary(sql_query: str, intermediate_applicants: pd.DataFrame) -> pd.DataFrame:
    """Process applicants data to primary layer."""
    return process_sql_to_parquet(sql_query, intermediate_applicants)

def process_vagas_primary(sql_query: str, intermediate_vagas: pd.DataFrame) -> pd.DataFrame:
    """Process job positions data to primary layer."""
    return process_sql_to_parquet(sql_query, intermediate_vagas)

def process_prospects_primary(sql_query: str, intermediate_prospects: pd.DataFrame) -> pd.DataFrame:
    """Process prospects data to primary layer."""
    return process_sql_to_parquet(sql_query, intermediate_prospects)

def process_primary_tables(
    applicants_sql: str,
    prospects_sql: str,
    vagas_sql: str,
    intermediate_applicants: pd.DataFrame,
    intermediate_prospects: pd.DataFrame,
    intermediate_vagas: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """
    Process all individual tables to primary layer in a single node.

    Args:
        applicants_sql: SQL query for applicants
        prospects_sql: SQL query for prospects
        vagas_sql: SQL query for job positions
        intermediate_applicants: Intermediate applicants DataFrame
        intermediate_prospects: Intermediate prospects DataFrame
        intermediate_vagas: Intermediate job positions DataFrame

    Returns:
        Dictionary containing processed DataFrames
    """
    logger.info("Processing all tables to primary layer...")
    
    # Process each table
    primary_applicants = process_sql_to_parquet(applicants_sql, intermediate_applicants)
    logger.info(f"Processed applicants: {len(primary_applicants)} rows")
    
    primary_prospects = process_sql_to_parquet(prospects_sql, intermediate_prospects)
    logger.info(f"Processed prospects: {len(primary_prospects)} rows")
    
    primary_vagas = process_sql_to_parquet(vagas_sql, intermediate_vagas)
    logger.info(f"Processed job positions: {len(primary_vagas)} rows")
    
    return {
        "primary_applicants": primary_applicants,
        "primary_prospects": primary_prospects,
        "primary_vagas": primary_vagas
    }

def process_core_primary(
    sql_query: str,
    primary_applicants: pd.DataFrame,
    primary_vagas: pd.DataFrame,
    primary_prospects: pd.DataFrame
) -> pd.DataFrame:
    """Process core integrated data to primary layer."""
    con = None
    try:
        con = duckdb.connect(database=':memory:')
        
        # Register all necessary tables
        con.register('primary_applicants', primary_applicants)
        con.register('primary_vagas', primary_vagas)
        con.register('primary_prospects', primary_prospects)

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
