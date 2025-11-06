import groovy.json.JsonOutput

def strategy = Jenkins.instance.getAuthorizationStrategy()
def roles = [:]

// Определяем, какие именно классы доступны
def RoleType, RoleStrategyClass

try {
    RoleType = this.class.classLoader.loadClass('com.synopsys.arc.jenkins.plugins.rolestrategy.RoleType')
    RoleStrategyClass = this.class.classLoader.loadClass('com.synopsys.arc.jenkins.plugins.rolestrategy.RoleBasedAuthorizationStrategy')
} catch (Throwable t1) {
    try {
        RoleType = this.class.classLoader.loadClass('com.michelin.cio.hudson.plugins.rolestrategy.RoleType')
        RoleStrategyClass = this.class.classLoader.loadClass('com.michelin.cio.hudson.plugins.rolestrategy.RoleBasedAuthorizationStrategy')
    } catch (Throwable t2) {
        return JsonOutput.toJson([error: "Role-Strategy plugin не найден"])
    }
}

if (!RoleStrategyClass.isInstance(strategy)) {
    return JsonOutput.toJson([error: "Role-Strategy не используется"])
}

// Функция для сбора ролей по типу
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
