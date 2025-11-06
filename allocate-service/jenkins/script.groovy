import com.michelin.cio.hudson.plugins.rolestrategy.*
import groovy.json.JsonOutput

def strategy = Jenkins.instance.getAuthorizationStrategy()
if (!(strategy instanceof RoleBasedAuthorizationStrategy)) {
    return JsonOutput.toJson([error: "Role Strategy plugin not used"])
}

def roles = [:]

// global roles
roles.global = strategy.getRoleMap(RoleBasedAuthorizationStrategy.GLOBAL).getRoles().collect { r ->
    [
        name: r.getName(),
        permissions: r.getPermissions()*.id,
        sids: strategy.getRoleMap(RoleBasedAuthorizationStrategy.GLOBAL).getSidsForRole(r.getName())
    ]
}

// project roles
roles.project = strategy.getRoleMap(RoleBasedAuthorizationStrategy.PROJECT).getRoles().collect { r ->
    [
        name: r.getName(),
        permissions: r.getPermissions()*.id,
        sids: strategy.getRoleMap(RoleBasedAuthorizationStrategy.PROJECT).getSidsForRole(r.getName())
    ]
}

// folder roles (если есть)
roles.folder = strategy.getRoleMap(RoleBasedAuthorizationStrategy.ITEM).getRoles().collect { r ->
    [
        name: r.getName(),
        permissions: r.getPermissions()*.id,
        sids: strategy.getRoleMap(RoleBasedAuthorizationStrategy.ITEM).getSidsForRole(r.getName())
    ]
}

JsonOutput.toJson(roles)
