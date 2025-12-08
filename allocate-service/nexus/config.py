import os
from dotenv import load_dotenv

load_dotenv()

NEXUS_URL = os.getenv("NEXUS_URL")
NEXUS_USER = os.getenv("NEXUS_USER")
NEXUS_PASS = os.getenv("NEXUS_PASS")


DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")


MAX_SESSION_INTERVAL_MIN = int(os.getenv("MAX_SESSION_INTERVAL_MIN", 5))
MERGE_SESSION_GAP_MIN = int(os.getenv("MERGE_SESSION_GAP_MIN", 1))
