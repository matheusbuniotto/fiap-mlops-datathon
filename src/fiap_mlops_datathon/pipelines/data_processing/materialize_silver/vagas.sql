SELECT
    id AS vaga_id,
    titulo_vaga,
    cliente,
    vaga_sap,
    nivel_profissional AS nivel_profissional_vaga,
    nivel_academico AS nivel_academico_vaga,
    nivel_ingles AS nivel_ingles_vaga,
    nivel_espanhol AS nivel_espanhol_vaga,
    areas_atuacao,
    principais_atividades,
    competencias,
    estado,
    cidade
FROM read_parquet('data/02_intermediate/vagas.parquet')
