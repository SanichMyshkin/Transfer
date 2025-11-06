import groovy.json.JsonOutput

def strategy = Jenkins.instance.getAuthorizationStrategy()
def roles = [:]
def RoleType
def RoleBasedAuthorizationStrategy

// Проверяем, какой класс доступен
try {
    RoleType = com.synopsys.arc.jenkins.plugins.rolestrategy.RoleType
    RoleBasedAuthorizationStrategy = com.synopsys.arc.jenkins.plugins.rolestrategy.RoleBasedAuthorizationStrategy
} catch (Throwable e1) {
    try {
        RoleType = com.michelin.cio.hudson.plugins.rolestrategy.RoleType
        RoleBasedAuthorizationStrategy = com.michelin.cio.hudson.plugins.rolestrategy.RoleBasedAuthorizationStrategy
    } catch (Throwable e2) {
        return JsonOutput.toJson([error: "Role Strategy plugin не найден"])
    }
}

if (!(strategy instanceof RoleBasedAuthorizationStrategy)) {
    return JsonOutput.toJson([error: "Role Strategy не используется"])
}

def collectRoles = { roleType ->
    def roleMap = strategy.getRoleMap(roleType)
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
