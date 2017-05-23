package common

import (
	"fmt"
	"net/http"

	"github.com/json-iterator/go"
)

// GlobalFlag use to contain the all context
var GlobalFlag *globalFlag

type globalFlag struct {
	sdaServiceName string
	keystoneURL    string
	sdaEndPoint    string
	token          string
}

type service struct {
	Enabled bool
	ID      string
	Name    string
	Type    string
}

type endpoint struct {
	URL       string
	Region    string
	Enable    bool
	Interface string
	ServiceID string
	ID        string
}

type version struct {
	Status string
	ID     string
	URL    []string
}

// getServiceEndPoint if one services have many endpoint, return the first one
func (g *globalFlag) getServiceEndPoint(serviceName string) (string, error) {

	var (
		serviceOBJ     service
		endpoints      []endpoint
		currentVersion version
	)

	c := g.getKeystoneClient()

	// get the service_id by service name
	request := Request{
		URL:          fmt.Sprintf("%s/services", c.URL),
		Method:       http.MethodGet,
		OkStatusCode: http.StatusOK,
	}

	resp, err := c.DoRequest(request)
	if err != nil {
		return "", err
	}

	iter := jsoniter.ParseBytes(resp.Body)
	for l1Field := iter.ReadObject(); l1Field != ""; l1Field = iter.ReadObject() {
		switch l1Field {
		case "services":
			for iter.ReadArray() {
				s := service{}
				for l2Field := iter.ReadObject(); l2Field != ""; l2Field = iter.ReadObject() {
					switch l2Field {
					case "description":
						iter.Skip()
					case "links":
						iter.Skip()
					case "enabled":
						s.Enabled = iter.ReadBool()
					case "type":
						s.Type = iter.ReadString()
					case "id":
						s.ID = iter.ReadString()
					case "name":
						s.Name = iter.ReadString()
					default:
						return "", fmt.Errorf("Get Keystone service error, Unkwon field: %s", l2Field)

					}
				}
				if s.Enabled && s.Name == serviceName {
					serviceOBJ = s
				}
			}
		case "links":
			iter.Skip()
		}
	}

	// get this service's all endpoints
	if serviceOBJ.ID != "" {
		request := Request{
			URL:          fmt.Sprintf("%s/endpoints?service_id=%s", c.URL, serviceOBJ.ID),
			Method:       http.MethodGet,
			OkStatusCode: http.StatusOK,
		}

		respEP, err := c.DoRequest(request)
		if err != nil {
			return "", err
		}

		iter := jsoniter.ParseBytes(respEP.Body)
		for l1Field := iter.ReadObject(); l1Field != ""; l1Field = iter.ReadObject() {
			switch l1Field {
			case "endpoints":
				for iter.ReadArray() {
					ep := endpoint{}
					for l2Field := iter.ReadObject(); l2Field != ""; l2Field = iter.ReadObject() {
						switch l2Field {
						case "region_id":
							iter.Skip()
						case "links":
							iter.Skip()
						case "url":
							ep.URL = iter.ReadString()
						case "region":
							ep.Region = iter.ReadString()
						case "enabled":
							ep.Enable = iter.ReadBool()
						case "interface":
							ep.Interface = iter.ReadString()
						case "service_id":
							ep.ServiceID = iter.ReadString()
						case "id":
							ep.ID = iter.ReadString()
						default:
							return "", fmt.Errorf("Get Keystone endpoint error, Unkwon field: %s", l2Field)
						}
					}
					if ep.Enable {
						endpoints = append(endpoints, ep)
					}
				}
			case "links":
				iter.Skip()
			}
		}
	}

	// if have many versin choice the current one
	requestROOT := Request{
		URL:          fmt.Sprintf("%s/versions", endpoints[0].URL),
		Method:       http.MethodGet,
		OkStatusCode: http.StatusOK,
	}

	respROOT, err := c.DoRequest(requestROOT)
	if err != nil {
		return "", err
	}

	iter = jsoniter.ParseBytes(respROOT.Body)
	for l1Field := iter.ReadObject(); l1Field != ""; l1Field = iter.ReadObject() {
		switch l1Field {
		case "versions":
			for iter.ReadArray() {
				v := version{}
				for l2Field := iter.ReadObject(); l2Field != ""; l2Field = iter.ReadObject() {
					switch l2Field {
					case "status":
						v.Status = iter.ReadString()
					case "id":
						v.ID = iter.ReadString()
					case "links":
						for iter.ReadArray() {
							for l3Field := iter.ReadObject(); l3Field != ""; l3Field = iter.ReadObject() {
								switch l3Field {
								case "href":
									v.URL = append(v.URL, iter.ReadString())
								case "rel":
									iter.Skip()
								}
							}
						}
					default:
						return "", fmt.Errorf("Get service current version error, Unkwon field: %s", l2Field)

					}
				}
				if v.Status == "CURRENT" {
					currentVersion = v
				}
			}
		}
	}

	if len(currentVersion.URL) != 0 {
		return currentVersion.URL[0], nil
	}

	return "", fmt.Errorf("not endpoint find for %s", serviceName)
}

func (g *globalFlag) SetToken(token string) {
	g.token = token
}

func (g *globalFlag) SetKeystoneURL(url string) {
	g.keystoneURL = url
}

func (g *globalFlag) SetSDAServiceName(name string) {
	g.sdaServiceName = name
}

func (g *globalFlag) GetToken() string {
	return g.token
}

func (g *globalFlag) SetSDAEndPoint(endpoint string) {
	g.sdaEndPoint = endpoint
}

func (g *globalFlag) getKeystoneClient() *Client {
	client, _ := NewClient(g.keystoneURL, g.token)
	return client
}

func (g *globalFlag) GetSDAClient() (*Client, error) {

	var (
		endpoint string
		err      error
	)

	if g.sdaEndPoint == "" {
		endpoint, err = g.getServiceEndPoint(g.sdaServiceName)
		if err != nil {
			return nil, err
		}
	} else {
		endpoint = g.sdaEndPoint
	}

	client, err := NewClient(endpoint, g.token)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func init() {
	if GlobalFlag == nil {
		GlobalFlag = &globalFlag{}
	}
}
