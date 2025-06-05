SELECT
    -- Informações da vaga
    v.vaga_id::INT AS vaga_id,
    v.titulo_vaga,
    v.cliente,
    CASE WHEN v.vaga_sap ILIKE 'sim' THEN 1 ELSE 0 END AS vaga_sap,
    v.nivel_profissional_vaga,
    v.nivel_academico_vaga,
    v.nivel_ingles_vaga,
    v.nivel_espanhol_vaga,
    v.areas_atuacao,
    v.principais_atividades,
    v.competencias,
    v.estado,
    v.cidade,

    -- Informações da prospecção
    p.prospect_id::INT AS prospect_id,
    p.codigo::INT AS codigo,
    p.nome_candidato,
    p.situacao_candidado,
    p.data_candidatura,
    p.ultima_atualizacao,
    p.comentario,
    p.recrutador,


    -- Informações do candidato
    a.prospect_codigo::INT AS prospect_codigo,
    a.nome,
    a.email,
    a.area_atuacao,
    a.nivel_profissional_candidato,
    a.nivel_academico_candidato,
    a.nivel_ingles_candidato,
    a.nivel_espanhol_candidato,
    a.conhecimentos_tecnicos,
    a.cv_pt,

    -- Target
    CASE
        WHEN situacao_candidado LIKE '%Contratado%' THEN 1
        ELSE 0
    END AS target_contratado

FROM read_parquet('data/03_primary/vagas.parquet') v
LEFT JOIN read_parquet('data/03_primary/prospects.parquet') p
    ON v.vaga_id = p.prospect_id
LEFT JOIN read_parquet('data/03_primary/applicants.parquet') a
    ON p.codigo = a.prospect_codigo
