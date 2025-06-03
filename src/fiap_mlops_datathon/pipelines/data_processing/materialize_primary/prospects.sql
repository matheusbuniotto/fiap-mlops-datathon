SELECT
    prospect_id,
    nome AS nome_candidato,
    codigo,
    situacao_candidado,
    data_candidatura,
    ultima_atualizacao,
    comentario,
    recrutador
FROM read_parquet('data/02_intermediate/prospects.parquet')
