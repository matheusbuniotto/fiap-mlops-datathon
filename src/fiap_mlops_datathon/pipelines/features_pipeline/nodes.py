import json
import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def load_json_from_text(json_text: str) -> Dict[str, Any]:
    """
    Load JSON data from text string.
    
    Args:
        json_text: JSON content as string
        
    Returns:
        Parsed JSON data
    """
    try:
        return json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {str(e)}")
        raise


def clean_empty_values(value: Any) -> Any:
    """
    Convert empty strings and other empty values to None for pandas compatibility.
    
    Args:
        value: Value to clean
        
    Returns:
        Cleaned value (None if empty, original value otherwise)
    """
    if value is None or pd.isna(value):
        return None
    elif isinstance(value, str):
        # Replace empty strings and whitespace-only strings with None
        return None if value.strip() == '' else value
    elif isinstance(value, (list, dict)):
        # Replace empty collections with None
        return None if len(value) == 0 else value
    else:
        return value


def optimize_dataframe_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize DataFrame data types to reduce memory usage and handle nulls properly.
    
    Args:
        df: DataFrame to optimize
        
    Returns:
        DataFrame with optimized data types
    """
    logger.info("Optimizing DataFrame data types...")
    optimized_df = df.copy()

    # First, replace empty values with proper NaN/None values
    optimized_df = optimized_df.replace('', np.nan)
    optimized_df = optimized_df.replace(r'^\s*$', np.nan, regex=True)  # Whitespace-only strings

    for col in optimized_df.columns:
        col_type = optimized_df[col].dtype

        # Optimize categorical strings
        if col_type == 'object':
            # Only convert to category if we have non-null values
            if optimized_df[col].notna().any():
                unique_ratio = optimized_df[col].nunique() / optimized_df[col].notna().sum()
                if unique_ratio < 0.5:  # If less than 50% unique values among non-null
                    optimized_df[col] = optimized_df[col].astype('category')

        # Optimize integers (pandas handles nullable integers with Int64, etc.)
        elif col_type in ['int64', 'float64']:
            # Check if column has any null values
            has_nulls = optimized_df[col].isna().any()

            if col_type == 'int64':
                if has_nulls:
                    # Use nullable integer types to preserve NaN values
                    optimized_df[col] = optimized_df[col].astype('Int64')
                else:
                    col_min = optimized_df[col].min()
                    col_max = optimized_df[col].max()

                    if col_min >= 0:
                        if col_max < 255:
                            optimized_df[col] = optimized_df[col].astype('uint8')
                        elif col_max < 65535:
                            optimized_df[col] = optimized_df[col].astype('uint16')
                        elif col_max < 4294967295:
                            optimized_df[col] = optimized_df[col].astype('uint32')
                    elif col_min > -128 and col_max < 127:
                        optimized_df[col] = optimized_df[col].astype('int8')
                    elif col_min > -32768 and col_max < 32767:
                        optimized_df[col] = optimized_df[col].astype('int16')
                    elif col_min > -2147483648 and col_max < 2147483647:
                        optimized_df[col] = optimized_df[col].astype('int32')

    logger.info("DataFrame optimization completed")
    return optimized_df


def process_applicant_record(app_id: str, app_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single applicant record with null handling.
    
    Args:
        app_id: Applicant ID
        app_data: Applicant data dictionary
        
    Returns:
        Processed applicant record
    """
    basic_info = app_data.get('infos_basicas', {})
    personal_info = app_data.get('informacoes_pessoais', {})
    professional_info = app_data.get('informacoes_profissionais', {})
    education_info = app_data.get('formacao_e_idiomas', {})

    record = {
        'id': app_id,
        # Basic information
        'nome': clean_empty_values(basic_info.get('nome')),
        'email': clean_empty_values(basic_info.get('email')),
        'telefone': clean_empty_values(basic_info.get('telefone')),
        'telefone_recado': clean_empty_values(basic_info.get('telefone_recado')),
        'data_criacao': clean_empty_values(basic_info.get('data_criacao')),
        'data_atualizacao': clean_empty_values(basic_info.get('data_atualizacao')),
        'inserido_por': clean_empty_values(basic_info.get('inserido_por')),
        'codigo_profissional': clean_empty_values(basic_info.get('codigo_profissional')),
        'objetivo_profissional': clean_empty_values(basic_info.get('objetivo_profissional')),

        # Personal information
        'cpf': clean_empty_values(personal_info.get('cpf')),
        'data_nascimento': clean_empty_values(personal_info.get('data_nascimento')),
        'sexo': clean_empty_values(personal_info.get('sexo')),
        'estado_civil': clean_empty_values(personal_info.get('estado_civil')),
        'pcd': clean_empty_values(personal_info.get('pcd')),
        'endereco': clean_empty_values(personal_info.get('endereco')),
        'linkedin': clean_empty_values(personal_info.get('url_linkedin')),
        'facebook': clean_empty_values(personal_info.get('facebook')),
        'skype': clean_empty_values(personal_info.get('skype')),

        # Professional information
        'titulo_profissional': clean_empty_values(professional_info.get('titulo_profissional')),
        'area_atuacao': clean_empty_values(professional_info.get('area_atuacao')),
        'conhecimentos_tecnicos': clean_empty_values(professional_info.get('conhecimentos_tecnicos')),
        'certificacoes': clean_empty_values(professional_info.get('certificacoes')),
        'remuneracao': clean_empty_values(professional_info.get('remuneracao')),
        'nivel_profissional': clean_empty_values(professional_info.get('nivel_profissional')),

        # Education and languages
        'nivel_academico': clean_empty_values(education_info.get('nivel_academico')),
        'nivel_ingles': clean_empty_values(education_info.get('nivel_ingles')),
        'nivel_espanhol': clean_empty_values(education_info.get('nivel_espanhol')),
        'outro_idioma': clean_empty_values(education_info.get('outro_idioma')),

        # CV files
        'cv_pt': clean_empty_values(app_data.get('cv_pt')),
        'cv_en': clean_empty_values(app_data.get('cv_en'))
    }

    return record


def process_vaga_record(vaga_id: str, vaga_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single job position record with null handling.
    
    Args:
        vaga_id: Job position ID
        vaga_data: Job position data dictionary
        
    Returns:
        Processed job position record
    """
    try:
        basic_info = vaga_data.get('informacoes_basicas', {})
        profile_info = vaga_data.get('perfil_vaga', {})
        benefits_info = vaga_data.get('beneficios', {})

        record = {
            'id': vaga_id,
            # Basic information
            'titulo_vaga': clean_empty_values(basic_info.get('titulo_vaga')),
            'cliente': clean_empty_values(basic_info.get('cliente')),
            'solicitante_cliente': clean_empty_values(basic_info.get('solicitante_cliente')),
            'empresa_divisao': clean_empty_values(basic_info.get('empresa_divisao')),
            'requisitante': clean_empty_values(basic_info.get('requisitante')),
            'analista_responsavel': clean_empty_values(basic_info.get('analista_responsavel')),
            'tipo_contratacao': clean_empty_values(basic_info.get('tipo_contratacao')),
            'data_requicisao': clean_empty_values(basic_info.get('data_requicisao')),
            'limite_contratacao': clean_empty_values(basic_info.get('limite_esperado_para_contratacao')),
            'vaga_sap': clean_empty_values(basic_info.get('vaga_sap')),
            'prazo_contratacao': clean_empty_values(basic_info.get('prazo_contratacao')),
            'objetivo_vaga': clean_empty_values(basic_info.get('objetivo_vaga')),
            'prioridade_vaga': clean_empty_values(basic_info.get('prioridade_vaga')),
            'origem_vaga': clean_empty_values(basic_info.get('origem_vaga')),

            # Job profile
            'pais': clean_empty_values(profile_info.get('pais')),
            'estado': clean_empty_values(profile_info.get('estado')),
            'cidade': clean_empty_values(profile_info.get('cidade')),
            'bairro': clean_empty_values(profile_info.get('bairro')),
            'local_trabalho': clean_empty_values(profile_info.get('local_trabalho')),
            'vaga_pcd': clean_empty_values(profile_info.get('vaga_especifica_para_pcd')),
            'faixa_etaria': clean_empty_values(profile_info.get('faixa_etaria')),
            'horario_trabalho': clean_empty_values(profile_info.get('horario_trabalho')),
            'nivel_profissional': clean_empty_values(profile_info.get('nivel profissional')),
            'nivel_academico': clean_empty_values(profile_info.get('nivel_academico')),
            'nivel_ingles': clean_empty_values(profile_info.get('nivel_ingles')),
            'nivel_espanhol': clean_empty_values(profile_info.get('nivel_espanhol')),
            'outro_idioma': clean_empty_values(profile_info.get('outro_idioma')),
            'areas_atuacao': clean_empty_values(profile_info.get('areas_atuacao')),
            'principais_atividades': clean_empty_values(profile_info.get('principais_atividades')),
            'competencias': clean_empty_values(profile_info.get('competencia_tecnicas_e_comportamentais')),
            'observacoes': clean_empty_values(profile_info.get('demais_observacoes')),
            'viagens_requeridas': clean_empty_values(profile_info.get('viagens_requeridas')),
            'equipamentos_necessarios': clean_empty_values(profile_info.get('equipamentos_necessarios')),

            # Benefits
            'valor_venda': clean_empty_values(benefits_info.get('valor_venda')),
            'valor_compra_1': clean_empty_values(benefits_info.get('valor_compra_1')),
            'valor_compra_2': clean_empty_values(benefits_info.get('valor_compra_2'))
        }

        return record

    except Exception as e:
        logger.error(f"Error in process_vaga_record for vaga_id {vaga_id}: {str(e)}")
        logger.error(f"Vaga data keys: {list(vaga_data.keys()) if isinstance(vaga_data, dict) else 'Not a dict'}")
        raise


def process_prospect_record(prospect_id: str, prospect_data: Dict[str, Any]) -> list:
    """
    Process prospect records with null handling.
    
    Args:
        prospect_id: Prospect ID
        prospect_data: Prospect data dictionary
        
    Returns:
        List of processed prospect records
    """
    titulo = clean_empty_values(prospect_data.get('titulo'))
    modalidade = clean_empty_values(prospect_data.get('modalidade'))
    prospects = prospect_data.get('prospects', [])

    records = []
    for prospect in prospects:
        record = {
            'prospect_id': prospect_id,
            'titulo': titulo,
            'modalidade': modalidade,
            'nome': clean_empty_values(prospect.get('nome')),
            'codigo': clean_empty_values(prospect.get('codigo')),
            'situacao_candidado': clean_empty_values(prospect.get('situacao_candidado')),
            'data_candidatura': clean_empty_values(prospect.get('data_candidatura')),
            'ultima_atualizacao': clean_empty_values(prospect.get('ultima_atualizacao')),
            'comentario': clean_empty_values(prospect.get('comentario')),
            'recrutador': clean_empty_values(prospect.get('recrutador'))
        }
        records.append(record)

    return records


def process_all_json_data(
    raw_applicants: dict,
    raw_vagas: dict,
    raw_prospects: dict
) -> Dict[str, pd.DataFrame]:
    """
    Process all JSON data into structured DataFrames with proper null handling and optimization.
    
    Args:
        raw_applicants: Raw applicants JSON data
        raw_vagas: Raw job positions JSON data
        raw_prospects: Raw prospects JSON data
        
    Returns:
        Dictionary containing processed and optimized DataFrames
    """
    logger.info("Processing all JSON data with null handling...")

    # Process applicants
    applicants_records = []
    for app_id, app_data in raw_applicants.items():
        try:
            record = process_applicant_record(app_id, app_data)
            applicants_records.append(record)
        except Exception as e:
            logger.error(f"Error processing applicant {app_id}: {str(e)}")
            continue

    # Process job positions
    vagas_records = []
    for vaga_id, vaga_data in raw_vagas.items():
        try:
            record = process_vaga_record(vaga_id, vaga_data)
            vagas_records.append(record)
        except Exception as e:
            logger.error(f"Error processing job position {vaga_id}: {str(e)}")
            continue

    # Process prospects
    prospects_records = []
    for prospect_id, prospect_data in raw_prospects.items():
        try:
            records = process_prospect_record(prospect_id, prospect_data)
            prospects_records.extend(records)
        except Exception as e:
            logger.error(f"Error processing prospect {prospect_id}: {str(e)}")
            continue

    # Create DataFrames
    applicants_df = pd.DataFrame(applicants_records)
    vagas_df = pd.DataFrame(vagas_records)
    prospects_df = pd.DataFrame(prospects_records)

    # Optimize DataFrames (this will also handle remaining null values properly)
    applicants_df = optimize_dataframe_types(applicants_df)
    vagas_df = optimize_dataframe_types(vagas_df)
    prospects_df = optimize_dataframe_types(prospects_df)

    # Log null statistics for monitoring
    logger.info(f"Processed applicants: {len(applicants_df)} records")
    logger.info(f"Applicants null values: {applicants_df.isnull().sum().sum()}")
    logger.info(f"Processed job positions: {len(vagas_df)} records")
    logger.info(f"Job positions null values: {vagas_df.isnull().sum().sum()}")
    logger.info(f"Processed prospects: {len(prospects_df)} records")
    logger.info(f"Prospects null values: {prospects_df.isnull().sum().sum()}")

    return {
        "intermediate_applicants": applicants_df,
        "intermediate_vagas": vagas_df,
        "intermediate_prospects": prospects_df
    }


# Additional utility function for Kedro pipeline
def validate_dataframe_nulls(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """
    Validate and report on null values in DataFrame for pipeline monitoring.
    
    Args:
        df: DataFrame to validate
        df_name: Name of the DataFrame for logging
        
    Returns:
        Same DataFrame (for pipeline chaining)
    """
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    null_percentage = (total_nulls / (len(df) * len(df.columns))) * 100

    logger.info(f"{df_name} validation:")
    logger.info(f"  Total records: {len(df)}")
    logger.info(f"  Total null values: {total_nulls}")
    logger.info(f"  Null percentage: {null_percentage:.2f}%")

    if null_percentage > 50:
        logger.warning(f"High null percentage ({null_percentage:.2f}%) in {df_name}")

    # Log columns with highest null counts
    high_null_cols = null_counts[null_counts > 0].sort_values(ascending=False).head(5)
    if len(high_null_cols) > 0:
        logger.info(f"  Columns with most nulls: {dict(high_null_cols)}")

    return df
