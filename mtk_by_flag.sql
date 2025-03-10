WITH RankedKeys AS (
    SELECT 
        [key], 
        split,
        ROW_NUMBER() OVER (PARTITION BY [key] ORDER BY split) AS rn
    FROM [dbo].[Impressions]
    WHERE DATEADD(MILLISECOND, time % 1000, DATEADD(SECOND, time / 1000, '1970-01-01')) >= CAST(DATEADD(DAY, -30, CAST(GETDATE() AS DATE)) AS DATETIME)
) 
SELECT TOP 25 
    split,
    COUNT(DISTINCT [key]) AS unique_key_count
FROM RankedKeys
WHERE rn = 1
GROUP BY split
ORDER BY unique_key_count DESC
