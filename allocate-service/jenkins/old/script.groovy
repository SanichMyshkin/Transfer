import hudson.model.User
import jenkins.security.LastGrantedAuthoritiesProperty
import groovy.json.JsonOutput

def users = User.getAll().collect { u ->
    def authProp = u.getProperty(LastGrantedAuthoritiesProperty)
    def authorities = authProp?.getAuthorities() ?: []
    [
        id: u.id,
        fullName: u.fullName,
        email: u.getProperty(hudson.tasks.Mailer.UserProperty)?.address ?: "",
        ad_groups: authorities
    ]
}

JsonOutput.toJson([users: users, total: users.size()])
