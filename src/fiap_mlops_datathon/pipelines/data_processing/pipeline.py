"""
SQL to Parquet processing pipeline definition.
This pipeline orchestrates the data processing flow.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import process_primary_tables, process_core_primary

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
                "applicants_sql": "sql_applicants",
                "prospects_sql": "sql_prospects",
                "vagas_sql": "sql_vagas",
                "intermediate_applicants": "intermediate_applicants",
                "intermediate_prospects": "intermediate_prospects",
                "intermediate_vagas": "intermediate_vagas"
            },
            outputs={
                "primary_applicants": "primary_applicants",
                "primary_prospects": "primary_prospects",
                "primary_vagas": "primary_vagas"
            },
            name="process_primary_tables_node",
            tags=["primary", "tables"]
        ),
        
        # Process final table with joins
        node(
            func=process_core_primary,
            inputs=[
                "sql_core_join",
                "primary_applicants",
                "primary_vagas", 
                "primary_prospects"
            ],
            outputs="primary_core",
            name="process_primary_core_node",
            tags=["primary", "core"]
        )
    ])

def create_pipeline(**kwargs) -> Pipeline:
    """
    Main pipeline creation function.
    
    Returns:
        Complete pipeline for the project
    """
    return create_primary_layer_pipeline()
