import com.michelin.cio.hudson.plugins.rolestrategy.*
import com.synopsys.arc.jenkins.plugins.rolestrategy.RoleType
import groovy.json.JsonOutput

def result = [:]
def auth = Jenkins.instance.getAuthorizationStrategy()

if (auth instanceof RoleBasedAuthorizationStrategy) {
    [RoleType.Global, RoleType.Project, RoleType.Slave].each { type ->
        def roleMap = auth.getRoleMap(type)
        def roles = [:]
        roleMap.getRoles().each { role ->
            roles[role.name] = roleMap.getSidsForRole(role.name)
        }
        result[type.name()] = roles
    }
}

println JsonOutput.prettyPrint(JsonOutput.toJson(result))
