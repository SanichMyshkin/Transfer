from database.utils.query_to_db import fetch_data

def fetch_docker_tags_data():
    query = """
        SELECT
            dc.name AS image_name,
            r.name AS repository,
            r.recipe_name AS repo_format,
            (r.attributes::jsonb -> 'storage' ->> 'blobStoreName') AS blob,
            COUNT(DISTINCT dc.version) AS tag_count
        FROM docker_component dc
        JOIN docker_content_repository dcr ON dc.repository_id = dcr.repository_id
        JOIN repository r ON dcr.config_repository_id = r.id
        GROUP BY
            dc.name,
            r.name,
            r.recipe_name,
            (r.attributes::jsonb -> 'storage' ->> 'blobStoreName');
    """
    return fetch_data(query)
