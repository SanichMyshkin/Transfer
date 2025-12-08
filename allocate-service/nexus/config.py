import os
from dotenv import load_dotenv

load_dotenv()

# Nexus API
NEXUS_URL = os.getenv("NEXUS_URL", "http://localhost:8081")
NEXUS_USER = os.getenv("NEXUS_USER", "admin")
NEXUS_PASS = os.getenv("NEXUS_PASS", "admin123")

# PostgreSQL (Nexus DB)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nexus")
DB_USER = os.getenv("DB_USER", "nexus")
DB_PASS = os.getenv("DB_PASS", "password")

# Архив с логами Nexus
ARCHIVE_PATH = os.getenv("NEXUS_AUDIT_ARCHIVE", "path/to/big_archive.zip")

REPORT_PATH = os.getenv("REPORT_PATH", "nexus_report.xlsx")

# LDAP / AD
AD_SERVER = os.getenv("AD_SERVER")
AD_USER = os.getenv("AD_USER")
AD_PASSWORD = os.getenv("AD_PASSWORD")
AD_BASE = os.getenv("AD_PEOPLE_SEARCH_BASE")
CA_CERT = os.getenv("CA_CERT", "CA.crt")
