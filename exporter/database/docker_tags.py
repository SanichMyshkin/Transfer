from database.utils.query_to_db import fetch_data

def fetch_docker_tags_data():
    query = """
        SELECT
            dc.name,
            dc.version,
            r.name,
            r.recipe_name,
            (r.attributes::jsonb -> 'storage' ->> 'blobStoreName')
        FROM
            docker_component dc
        JOIN
            docker_content_repository dcr ON dc.repository_id = dcr.repository_id
        JOIN
            repository r ON dcr.config_repository_id = r.id;
    """
    return fetch_data(query)
