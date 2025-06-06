"""
SQL to Parquet processing pipeline definition.
This pipeline orchestrates the data processing flow.
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import process_primary_tables


def create_primary_layer_pipeline(**kwargs) -> Pipeline:
    """
    Create the primary layer processing pipeline.

    Returns:
        Kedro pipeline for processing data to primary layer
    """
    return pipeline([
        # Process all individual tables in a single node
        node(
            func=process_primary_tables,
            inputs={
                "prospects_sql": "sql_prospects",
                "vagas_sql": "sql_vagas",
                "intermediate_prospects": "intermediate_prospects",
                "intermediate_vagas": "intermediate_vagas"
            },
            outputs={
                "primary_prospects": "primary_prospects",
                "primary_vagas": "primary_vagas"
            },
            name="process_primary_tables_node",
            tags=["primary", "tables"]
        )
    ])

def create_pipeline(**kwargs) -> Pipeline:
    """
    Main pipeline creation function.
    
    Returns:
        Complete pipeline for the project
    """
    return create_primary_layer_pipeline()
