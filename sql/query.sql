SELECT
    Age,
    COUNT(*) AS cnt
FROM titanic
WHERE Survived = 1
GROUP BY Age
ORDER BY cnt DESC
LIMIT 10;