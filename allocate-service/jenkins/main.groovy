import com.michelin.cio.hudson.plugins.rolestrategy.*
import groovy.json.JsonOutput

def roleMap = [:]
def auth = Jenkins.instance.getAuthorizationStrategy()

if (auth instanceof RoleBasedAuthorizationStrategy) {
    def globalRoles = auth.getRoleMap(RoleBasedAuthorizationStrategy.GLOBAL)
    globalRoles.getRoles().each { role ->
        def sids = globalRoles.getSidsForRole(role)
        roleMap[role.getName()] = sids
    }
}

println JsonOutput.prettyPrint(JsonOutput.toJson(roleMap))
