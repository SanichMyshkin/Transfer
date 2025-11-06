import groovy.json.JsonOutput

def strategy = Jenkins.instance.getAuthorizationStrategy()
def loader = Jenkins.instance.pluginManager.uberClassLoader

// пробуем старый пакет (Michelin)
def RoleType = loader.loadClass('com.michelin.cio.hudson.plugins.rolestrategy.RoleType')
def RoleBasedAuthorizationStrategy = loader.loadClass('com.michelin.cio.hudson.plugins.rolestrategy.RoleBasedAuthorizationStrategy')

if (!RoleBasedAuthorizationStrategy.isInstance(strategy)) {
    return JsonOutput.toJson([error: "Role-Strategy plugin есть, но не используется как текущая авторизация"])
}

def collectRoles = { roleType ->
    def roleMap = strategy.getRoleMap(roleType)
    roleMap.getRoles().collect { r ->
        [
            name: r.getName(),
            permissions: r.getPermissions()*.id,
            sids: roleMap.getSidsForRole(r.getName())
        ]
    }
}

def roles = [
    global : collectRoles(RoleType.Global),
    project: collectRoles(RoleType.Project),
    folder : collectRoles(RoleType.Item)
]

JsonOutput.toJson(roles)
