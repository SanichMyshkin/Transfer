import os
import json
import urllib3
from dotenv import load_dotenv
from jenkins_groovy import JenkinsGroovyClient

# --- Настройки ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

client = JenkinsGroovyClient(JENKINS_URL, USER, PASSWORD, is_https=False)

# --- Скрипт: пользователи ---
script_users = """
import jenkins.model.Jenkins
import hudson.model.User
import hudson.tasks.Mailer
import groovy.json.JsonOutput

def users = User.getAll()
def userList = users.collect { u ->
    def email = u.getProperty(Mailer.UserProperty)?.address ?: ""
    [
        id: u.id,
        fullName: u.fullName,
        email: email
    ]
}
def result = [users: userList, total: users.size()]
JsonOutput.toJson(result)
"""

# --- Скрипт: джобы ---
script_jobs = """
import jenkins.model.Jenkins
import groovy.json.JsonOutput

def jobs = Jenkins.instance.getAllItems()
def jobList = jobs.collect { j ->
    [
        name: j.fullName,
        url: j.absoluteUrl,
        type: j.class.simpleName,
        description: j.description ?: "",
        isBuildable: j.isBuildable(),
        lastBuild: j.getLastBuild()?.number,
        lastResult: j.getLastBuild()?.result?.toString()
    ]
}
def result = [jobs: jobList, total: jobs.size()]
JsonOutput.toJson(result)
"""

# --- Скрипт: ноды ---
script_nodes = """
import jenkins.model.Jenkins
import groovy.json.JsonOutput

def nodes = Jenkins.instance.nodes
def nodeList = nodes.collect { n ->
    [
        name: n.displayName,
        online: n.computer?.isOnline(),
        executors: n.numExecutors,
        labels: n.getLabelString(),
        mode: n.mode?.toString(),
        description: n.nodeDescription ?: ""
    ]
}
def result = [nodes: nodeList, total: nodes.size()]
JsonOutput.toJson(result)
"""

# --- Выполнение ---
print("🔹 Получаем пользователей...")
users = client.run_script(script_users)
print(f"  → {users['total']} пользователей")

print("🔹 Получаем джобы...")
jobs = client.run_script(script_jobs)
print(f"  → {jobs['total']} джоб")

print("🔹 Получаем ноды...")
nodes = client.run_script(script_nodes)
print(f"  → {nodes['total']} нод")

# --- Сохраняем всё в JSON ---
inventory = {
    "users": users["users"],
    "jobs": jobs["jobs"],
    "nodes": nodes["nodes"],
}

with open("jenkins_inventory.json", "w", encoding="utf-8") as f:
    json.dump(inventory, f, ensure_ascii=False, indent=2)

print("\n✅ Готово! Данные сохранены в jenkins_inventory.json")
