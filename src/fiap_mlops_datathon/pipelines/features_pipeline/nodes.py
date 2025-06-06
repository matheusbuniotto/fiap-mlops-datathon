import json
import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

def create_alternative_ranking_system() -> Dict[str, int]:
    """
    Alternative ranking system using only positive scores (0-10 scale).
    Some algorithms prefer non-negative rankings.

    Returns:
        Dictionary mapping status to ranking score (0-10)
    """
    ranking_system = {
        # TIER 1: Sucess
        'Contratado pela Decision': 10,
        'Contratado como Hunting': 9,
        'Proposta Aceita': 8,

        # TIER 2: Final Stages of hiring process
        'Aprovado': 7,
        'Documentação PJ': 6,
        'Documentação CLT': 6,
        'Documentação Cooperado': 6,
        'Encaminhar Proposta': 5,

        # TIER 3: Advanced stages
        'Entrevista com Cliente': 4,
        'Entrevista Técnica': 4,
        'Encaminhado ao Requisitante': 3,

        # TIER 4: Early stages
        'Em avaliação pelo RH': 2,
        'Inscrito': 2,

        # TIER 5: Bad outcomes
        'Desistiu da Contratação': 1,
        'Desistiu': 1,
        'Sem interesse nesta vaga': 1,

        # TIER 6: Rejections
        'Não Aprovado pelo Cliente': 0,
        'Não Aprovado pelo RH': 0,
        'Não Aprovado pelo Requisitante': 0,
        'Recusado': 0
    }

    return ranking_system

def dataset_read(parquet_path: str) -> pd.DataFrame:
    """
    Read a Parquet dataset.

    Args:
        parquet_path: Path to the Parquet file

    Returns:
        DataFrame containing the dataset
    """
    try:
        df = pd.read_parquet(parquet_path)
        logger.info(f"Dataset loaded successfully from {parquet_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read dataset from {parquet_path}: {e}")
        raise

x = dataset_read(parquet_path="../../data/03_primary/core_applicants_jobs_prospects.parquet")

print(x.head())

