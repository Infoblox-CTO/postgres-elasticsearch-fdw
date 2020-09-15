SELECT
    DISTINCT (id = 39401194 AND title = 'Parel van de Veluwe')
FROM
    articles_es
WHERE
    body LIKE '%Godert de Leeuw%' AND id = 39401194
;
