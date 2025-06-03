"""
SQL to Parquet processing pipeline definition.
This pipeline orchestrates the data processing flow.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import process_silver_tables, process_core_silver

def create_silver_layer_pipeline(**kwargs) -> Pipeline:
    """
    Create the silver layer processing pipeline.

    Returns:
        Kedro pipeline for processing data to silver layer
    """
    return pipeline([
        # Process all individual tables in a single node
        node(
            func=process_silver_tables,
            inputs={
                "applicants_sql": "sql_applicants",
                "prospects_sql": "sql_prospects",
                "vagas_sql": "sql_vagas",
                "bronze_applicants": "bronze_applicants",
                "bronze_prospects": "bronze_prospects",
                "bronze_vagas": "bronze_vagas"
            },
            outputs={
                "silver_applicants": "silver_applicants",
                "silver_prospects": "silver_prospects",
                "silver_vagas": "silver_vagas"
            },
            name="process_silver_tables_node",
            tags=["silver", "tables"]
        ),
        
        # Process final table with joins
        node(
            func=process_core_silver,
            inputs=[
                "sql_core",
                "silver_applicants",
                "silver_vagas", 
                "silver_prospects"
            ],
            outputs="silver_core",
            name="process_silver_core_node",
            tags=["silver", "core"]
        )
    ])

def create_pipeline(**kwargs) -> Pipeline:
    """
    Main pipeline creation function.
    
    Returns:
        Complete pipeline for the project
    """
    return create_silver_layer_pipeline()
