SELECT 
    distinct split,
    treatment
FROM [dbo].[Impressions]
WHERE [key] like 'dmartin%'
ORDER BY split, treatment