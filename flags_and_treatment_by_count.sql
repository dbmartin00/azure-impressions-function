SELECT DISTINCT 
    split,
    treatment,
    COUNT(*) AS record_count,
    DATEADD(MILLISECOND, MAX(time) % 1000, DATEADD(SECOND, MAX(time) / 1000, '1970-01-01')) AS lastEvaluated
FROM [dbo].[Impressions]
    WHERE DATEADD(MILLISECOND, time % 1000, DATEADD(SECOND, time / 1000, '1970-01-01')) >= CAST(DATEADD(DAY, -30, CAST(GETDATE() AS DATE)) AS DATETIME)
GROUP BY split, treatment
ORDER BY record_count DESC;
