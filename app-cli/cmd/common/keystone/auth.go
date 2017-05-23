package keystone

// Domain keystone Domain
type Domain struct {
	Name string `json:"name"`
}

// IdentifyUser keystone user
type IdentifyUser struct {
	User User `json:"user"`
}

// User keystone user
type User struct {
	Name     string `json:"name"`
	Password string `json:"password"`
	Domain   Domain `json:"domain"`
}

// Identity keystone ID
type Identity struct {
	Methods  []string     `json:"methods"`
	Password IdentifyUser `json:"password"`
}
type Scope struct {
	Project Project `json:"project"`
}
type Project struct {
	Name   string `json:"name"`
	Domain Domain `json:"domain"`
}

// Auth with ID
type Auth struct {
	Identity Identity `json:"identity"`
	Scope    Scope    `json:"scope"`
}

// SingleAuth for password
type SingleAuth struct {
	Auth Auth `json:"auth"`
}

// NewAuth use to new auth
func NewAuth(username, password, domainName, projectName string) Auth {
	return Auth{
		Identity: Identity{
			Methods: []string{"password"},
			Password: IdentifyUser{
				User: User{
					Name:     username,
					Password: password,
					Domain:   Domain{Name: domainName},
				},
			},
		},
		Scope: Scope{
			Project: Project{
				Name:   projectName,
				Domain: Domain{Name: domainName},
			},
		},
	}
}
