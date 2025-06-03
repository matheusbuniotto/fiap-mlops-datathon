"""Project pipelines."""
from __future__ import annotations

from typing import Dict
from kedro.pipeline import Pipeline
from fiap_mlops_datathon.pipelines.json_processing import create_pipeline as json_processing
from fiap_mlops_datathon.pipelines.data_processing import create_pipeline as data_processing

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines."""
    json_processing_pipeline = json_processing()
    silver_processing_pipeline = data_processing()
    
    return {
        "__default__": json_processing_pipeline + silver_processing_pipeline,
        "json_processing": json_processing_pipeline,
        "silver_processing": silver_processing_pipeline,
        "full": json_processing_pipeline + silver_processing_pipeline,
    }