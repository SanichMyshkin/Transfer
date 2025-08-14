from database.utils.query_to_db import fetch_data

def fetch_cleanup_name():
    query = """
        SELECT name 
        FROM cleanup_policy
    """
    return fetch_data(query)
