package common

// GlobalFlag use to contain the all context
var GlobalFlag *globalFlag

type globalFlag struct {
	endpoint string
	token    string
}

func (g *globalFlag) SetToken(token string) {
	g.token = token
}

func (g *globalFlag) GetToken() string {
	return g.token
}

func (g *globalFlag) SetEndPoint(url string) {
	g.endpoint = url
}

func (g *globalFlag) GetEndPoint() string {
	return g.endpoint
}

func (g *globalFlag) GetClient() *Client {
	client, _ := NewClient(g.endpoint, g.token)
	return client
}

func init() {
	if GlobalFlag == nil {
		GlobalFlag = &globalFlag{}
	}
}
