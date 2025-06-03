"""
JSON to Parquet processing pipeline definition.
This pipeline orchestrates the data processing flow.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import process_all_json_data

def create_json_to_parquet_pipeline(**kwargs) -> Pipeline:
    """
    Create the JSON to Parquet processing pipeline.
    
    Returns:
        Kedro pipeline for processing JSON files to Parquet format
    """
    return pipeline([
        # Process and optimize all data in a single node
        node(
            func=process_all_json_data,
            inputs={
                "raw_applicants": "raw_applicants",
                "raw_vagas": "raw_vagas",
                "raw_prospects": "raw_prospects"
            },
            outputs={
                "bronze_applicants": "bronze_applicants",
                "bronze_vagas": "bronze_vagas",
                "bronze_prospects": "bronze_prospects"
            },
            name="process_bronze_data_node",
            tags=["processing", "bronze"]
        )
    ])

def create_pipeline(**kwargs) -> Pipeline:
    """
    Main pipeline creation function.
    
    Returns:
        Complete pipeline for the project
    """
    return create_json_to_parquet_pipeline()