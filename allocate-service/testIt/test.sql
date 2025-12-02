SELECT
    p."Id",
    p."Name",
    COALESCE(SUM(tr."RunCount"), 0) AS "AutoTestRunsCount"
FROM "Projects" p
LEFT JOIN "TestRuns" tr
       ON tr."ProjectId" = p."Id"
      AND tr."IsAutomated" = TRUE
      AND (tr."IsDeleted" = FALSE OR tr."IsDeleted" IS NULL)
GROUP BY p."Id", p."Name"
ORDER BY p."Id";
