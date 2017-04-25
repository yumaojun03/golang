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

// Auth with ID
type Auth struct {
	Identity Identity `json:"identity"`
}

// SingleAuth for password
type SingleAuth struct {
	Auth Auth `json:"auth"`
}

// NewAuth use to new auth
func NewAuth(username, password, domainName string) Auth {
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
	}
}
