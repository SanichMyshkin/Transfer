SELECT
    p."Id" AS project_id,
    p."Name" AS project_name,
    u."Id" AS owner_id,
    u."UserName" AS owner_username,
    u."Email" AS owner_email,
    u."FirstName",
    u."LastName"
FROM
    "Projects" p
    LEFT JOIN "AspNetUsers" u ON u."Id" = p."CreatedBy"
ORDER BY
    p."Id";