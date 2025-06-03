SELECT
    id AS prospect_codigo,
    nome,
    email,
    area_atuacao,
    nivel_profissional AS nivel_profissional_candidato,
    nivel_academico AS nivel_academico_candidato,
    nivel_ingles AS nivel_ingles_candidato,
    nivel_espanhol AS nivel_espanhol_candidato,
    conhecimentos_tecnicos,
    cv_pt
FROM read_parquet('data/02_intermediate/applicants.parquet')
