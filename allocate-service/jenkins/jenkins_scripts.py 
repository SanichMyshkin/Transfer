# -*- coding: utf-8 -*-

# --- Получение пользователей ---
SCRIPT_USERS = """
import hudson.model.User
import hudson.tasks.Mailer
import groovy.json.JsonOutput

def users = User.getAll()
def userList = users.collect { u ->
    def email = u.getProperty(Mailer.UserProperty)?.address ?: ""
    [id: u.id, fullName: u.fullName, email: email]
}
JsonOutput.toJson([users: userList, total: users.size()])
"""

# --- Получение джоб ---
SCRIPT_JOBS = """
import jenkins.model.Jenkins
import groovy.json.JsonOutput

def jobs = Jenkins.instance.getAllItems()
def jobList = jobs.collect { j ->
    def info = [
        name: j.fullName,
        url: j.absoluteUrl,
        type: j.class.simpleName,
        description: j.description ?: "",
        isBuildable: (j.metaClass.respondsTo(j, "isBuildable") ? j.isBuildable() : false),
        isFolder: j.class.simpleName.contains("Folder")
    ]
    try {
        if (j.metaClass.respondsTo(j, "getLastBuild")) {
            def lb = j.getLastBuild()
            if (lb) {
                info.lastBuild = lb.number
                info.lastResult = lb.result?.toString()
                info.lastBuildTime = lb.getTime()?.toString()
            }
        }
    } catch (Exception e) {
        info.error = e.message
    }
    return info
}
JsonOutput.toJson([jobs: jobList, total: jobs.size()])
"""

# --- Получение нод ---
SCRIPT_NODES = """
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
JsonOutput.toJson([nodes: nodeList, total: nodes.size()])
"""
