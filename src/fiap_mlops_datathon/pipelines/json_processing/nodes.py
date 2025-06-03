"""
Alternative nodes with JSON loading for text datasets.
"""

import json
import logging
from typing import Any, Dict
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


def optimize_dataframe_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize DataFrame data types to reduce memory usage.
    
    Args:
        df: DataFrame to optimize
        
    Returns:
        DataFrame with optimized data types
    """
    logger.info("Optimizing DataFrame data types...")
    optimized_df = df.copy()
    
    for col in optimized_df.columns:
        col_type = optimized_df[col].dtype
        
        # Optimize categorical strings
        if col_type == 'object':
            unique_ratio = optimized_df[col].nunique() / len(optimized_df)
            if unique_ratio < 0.5:  # If less than 50% unique values
                optimized_df[col] = optimized_df[col].astype('category')
        
        # Optimize integers
        elif col_type in ['int64']:
            col_min = optimized_df[col].min()
            col_max = optimized_df[col].max()
            
            if col_min >= 0:
                if col_max < 255:
                    optimized_df[col] = optimized_df[col].astype('uint8')
                elif col_max < 65535:
                    optimized_df[col] = optimized_df[col].astype('uint16')
                elif col_max < 4294967295:
                    optimized_df[col] = optimized_df[col].astype('uint32')
            else:
                if col_min > -128 and col_max < 127:
                    optimized_df[col] = optimized_df[col].astype('int8')
                elif col_min > -32768 and col_max < 32767:
                    optimized_df[col] = optimized_df[col].astype('int16')
                elif col_min > -2147483648 and col_max < 2147483647:
                    optimized_df[col] = optimized_df[col].astype('int32')
    
    logger.info("DataFrame optimization completed")
    return optimized_df


def process_all_json_data(
    raw_applicants: dict,
    raw_vagas: dict,
    raw_prospects: dict
) -> Dict[str, pd.DataFrame]:
    """
    Process all JSON data into structured DataFrames and optimize them.
    
    Args:
        raw_applicants: Raw applicants JSON data
        raw_vagas: Raw job positions JSON data
        raw_prospects: Raw prospects JSON data
        
    Returns:
        Dictionary containing processed and optimized DataFrames
    """
    logger.info("Processing all JSON data...")
    
    # Process applicants
    applicants_records = []
    for app_id, app_data in raw_applicants.items():
        try:
            basic_info = app_data.get('infos_basicas', {})
            personal_info = app_data.get('informacoes_pessoais', {})
            professional_info = app_data.get('informacoes_profissionais', {})
            education_info = app_data.get('formacao_e_idiomas', {})
            
            record = {
                'id': app_id,
                # Basic information
                'nome': basic_info.get('nome', ''),
                'email': basic_info.get('email', ''),
                'telefone': basic_info.get('telefone', ''),
                'telefone_recado': basic_info.get('telefone_recado', ''),
                'data_criacao': basic_info.get('data_criacao', ''),
                'data_atualizacao': basic_info.get('data_atualizacao', ''),
                'inserido_por': basic_info.get('inserido_por', ''),
                'codigo_profissional': basic_info.get('codigo_profissional', ''),
                'objetivo_profissional': basic_info.get('objetivo_profissional', ''),
                
                # Personal information
                'cpf': personal_info.get('cpf', ''),
                'data_nascimento': personal_info.get('data_nascimento', ''),
                'sexo': personal_info.get('sexo', ''),
                'estado_civil': personal_info.get('estado_civil', ''),
                'pcd': personal_info.get('pcd', ''),
                'endereco': personal_info.get('endereco', ''),
                'linkedin': personal_info.get('url_linkedin', ''),
                'facebook': personal_info.get('facebook', ''),
                'skype': personal_info.get('skype', ''),
                
                # Professional information
                'titulo_profissional': professional_info.get('titulo_profissional', ''),
                'area_atuacao': professional_info.get('area_atuacao', ''),
                'conhecimentos_tecnicos': professional_info.get('conhecimentos_tecnicos', ''),
                'certificacoes': professional_info.get('certificacoes', ''),
                'remuneracao': professional_info.get('remuneracao', ''),
                'nivel_profissional': professional_info.get('nivel_profissional', ''),
                
                # Education and languages
                'nivel_academico': education_info.get('nivel_academico', ''),
                'nivel_ingles': education_info.get('nivel_ingles', ''),
                'nivel_espanhol': education_info.get('nivel_espanhol', ''),
                'outro_idioma': education_info.get('outro_idioma', ''),
                
                # CV files
                'cv_pt': app_data.get('cv_pt', ''),
                'cv_en': app_data.get('cv_en', '')
            }
            applicants_records.append(record)
            
        except Exception as e:
            logger.error(f"Error processing applicant {app_id}: {str(e)}")
            continue
    
    # Process job positions
    vagas_records = []
    for vaga_id, vaga_data in raw_vagas.items():
        try:
            basic_info = vaga_data.get('informacoes_basicas', {})
            profile_info = vaga_data.get('perfil_vaga', {})
            benefits_info = vaga_data.get('beneficios', {})
            
            record = {
                'id': vaga_id,
                # Basic information
                'titulo_vaga': basic_info.get('titulo_vaga', ''),
                'cliente': basic_info.get('cliente', ''),
                'solicitante_cliente': basic_info.get('solicitante_cliente', ''),
                'empresa_divisao': basic_info.get('empresa_divisao', ''),
                'requisitante': basic_info.get('requisitante', ''),
                'analista_responsavel': basic_info.get('analista_responsavel', ''),
                'tipo_contratacao': basic_info.get('tipo_contratacao', ''),
                'data_requicisao': basic_info.get('data_requicisao', ''),
                'limite_contratacao': basic_info.get('limite_esperado_para_contratacao', ''),
                'vaga_sap': basic_info.get('vaga_sap', ''),
                'prazo_contratacao': basic_info.get('prazo_contratacao', ''),
                'objetivo_vaga': basic_info.get('objetivo_vaga', ''),
                'prioridade_vaga': basic_info.get('prioridade_vaga', ''),
                'origem_vaga': basic_info.get('origem_vaga', ''),
                
                # Job profile
                'pais': profile_info.get('pais', ''),
                'estado': profile_info.get('estado', ''),
                'cidade': profile_info.get('cidade', ''),
                'bairro': profile_info.get('bairro', ''),
                'local_trabalho': profile_info.get('local_trabalho', ''),
                'vaga_pcd': profile_info.get('vaga_especifica_para_pcd', ''),
                'faixa_etaria': profile_info.get('faixa_etaria', ''),
                'horario_trabalho': profile_info.get('horario_trabalho', ''),
                'nivel_profissional': profile_info.get('nivel profissional', ''),
                'nivel_academico': profile_info.get('nivel_academico', ''),
                'nivel_ingles': profile_info.get('nivel_ingles', ''),
                'nivel_espanhol': profile_info.get('nivel_espanhol', ''),
                'outro_idioma': profile_info.get('outro_idioma', ''),
                'areas_atuacao': profile_info.get('areas_atuacao', ''),
                'principais_atividades': profile_info.get('principais_atividades', ''),
                'competencias': profile_info.get('competencia_tecnicas_e_comportamentais', ''),
                'observacoes': profile_info.get('demais_observacoes', ''),
                'viagens_requeridas': profile_info.get('viagens_requeridas', ''),
                'equipamentos_necessarios': profile_info.get('equipamentos_necessarios', ''),
                
                # Benefits
                'valor_venda': benefits_info.get('valor_venda', ''),
                'valor_compra_1': benefits_info.get('valor_compra_1', ''),
                'valor_compra_2': benefits_info.get('valor_compra_2', '')
            }
            vagas_records.append(record)
            
        except Exception as e:
            logger.error(f"Error processing job position {vaga_id}: {str(e)}")
            continue
    
    # Process prospects
    prospects_records = []
    for prospect_id, prospect_data in raw_prospects.items():
        try:
            titulo = prospect_data.get('titulo', '')
            modalidade = prospect_data.get('modalidade', '')
            prospects = prospect_data.get('prospects', [])
            
            for prospect in prospects:
                record = {
                    'prospect_id': prospect_id,
                    'titulo': titulo,
                    'modalidade': modalidade,
                    'nome': prospect.get('nome', ''),
                    'codigo': prospect.get('codigo', ''),
                    'situacao_candidado': prospect.get('situacao_candidado', ''),
                    'data_candidatura': prospect.get('data_candidatura', ''),
                    'ultima_atualizacao': prospect.get('ultima_atualizacao', ''),
                    'comentario': prospect.get('comentario', ''),
                    'recrutador': prospect.get('recrutador', '')
                }
                prospects_records.append(record)
                
        except Exception as e:
            logger.error(f"Error processing prospect {prospect_id}: {str(e)}")
            continue

    # Create and optimize DataFrames
    applicants_df = optimize_dataframe_types(pd.DataFrame(applicants_records))
    vagas_df = optimize_dataframe_types(pd.DataFrame(vagas_records))
    prospects_df = optimize_dataframe_types(pd.DataFrame(prospects_records))
    
    logger.info(f"Processed applicants: {len(applicants_df)} records")
    logger.info(f"Processed job positions: {len(vagas_df)} records")
    logger.info(f"Processed prospects: {len(prospects_df)} records")
    
    return {
        "bronze_applicants": applicants_df,
        "bronze_vagas": vagas_df,
        "bronze_prospects": prospects_df
    }