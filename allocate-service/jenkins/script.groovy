import com.synopsys.arc.jenkins.plugins.rolestrategy.*
import groovy.json.JsonOutput

def strategy = Jenkins.instance.getAuthorizationStrategy()
if (!(strategy instanceof RoleBasedAuthorizationStrategy)) {
    return JsonOutput.toJson([error: "Role Strategy plugin not active"])
}

def roles = [:]

def collectRoles = { RoleType type ->
    def roleMap = strategy.getRoleMap(type)
    return roleMap.getRoles().collect { r ->
        [
            name: r.getName(),
            permissions: r.getPermissions()*.id,
            sids: roleMap.getSidsForRole(r.getName())
        ]
    }
}

roles.global = collectRoles(RoleType.Global)
roles.project = collectRoles(RoleType.Project)
roles.folder = collectRoles(RoleType.Item)

JsonOutput.toJson(roles)
