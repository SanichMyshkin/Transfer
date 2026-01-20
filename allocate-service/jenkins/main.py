import os
import sys
import logging
import urllib3
from dotenv import load_dotenv
from jenkins_client import JenkinsGroovyClient
from jenkins_scripts import SCRIPT_JOBS, SCRIPT_NODES
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")
client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=False)


def get_jobs():
    logger.info("Получаем джобы...")
    data = client.run_script(SCRIPT_JOBS)
    logger.info(f"Джоб: {data['total']}")
    return data


def get_nodes():
    logger.info("Получаем ноды...")
    data = client.run_script(SCRIPT_NODES)
    logger.info(f"Нод: {data['total']}")
    return data


def get_sum_build_and_jobs(data):
    acc = defaultdict(lambda: {"jobs_sum": 0, "build_sum": 0})
    for j in data.get("jobs", []):
        if j.get("isFolder"):
            continue
        project_name = j.get("name", "").split("/", 1)[0]
        if not project_name:
            continue
        acc[project_name]["jobs_sum"] += 1
        last_build = j.get("lastBuild")
        if last_build is not None:
            acc[project_name]["build_sum"] += last_build
    return dict(acc)


def main():
    try:
        jobs = get_jobs()
        nodes = get_nodes()
        print(nodes)
        get_sum_build_and_jobs(jobs)
        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
