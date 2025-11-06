import jenkins.model.Jenkins
import com.cloudbees.hudson.plugins.folder.Folder
import groovy.json.JsonOutput

def folders = Jenkins.instance.getAllItems(Folder.class)
def result = []

folders.each { f ->
    def jobs = f.getAllJobs()
    def totalBuilds = 0
    jobs.each { j ->
        totalBuilds += j.getBuilds().size()
    }

    result << [
        folder: f.fullName,
        jobCount: jobs.size(),
        buildCount: totalBuilds
    ]
}

def rootJobs = Jenkins.instance.getItems()
def rootBuilds = 0
rootJobs.each { j ->
    if (j.metaClass.hasProperty(j, 'builds')) {
        rootBuilds += j.getBuilds().size()
    }
}
result << [folder: "/", jobCount: rootJobs.size(), buildCount: rootBuilds]

def totalBuildsAll = result.sum { it.buildCount }
JsonOutput.toJson([totalFolders: result.size(), totalBuilds: totalBuildsAll, folders: result])
