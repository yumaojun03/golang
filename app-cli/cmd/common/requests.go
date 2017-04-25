package common

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Request struct {
	URL          string
	Method       string
	Body         []byte
	OkStatusCode int
}

type Response struct {
	Body       []byte
	StatusCode int
	Headers    http.Header
}

type Client struct {
	URL   string
	Token string
}

func NewClient(url string, token string) (*Client, error) {
	if url == "" {
		return nil, errors.New("missing URL")
	}
	return &Client{URL: url, Token: token}, nil
}

func (c *Client) DoRequest(r Request) (Response, error) {
	client := &http.Client{}

	req, err := http.NewRequest(r.Method, r.URL, bytes.NewBuffer(r.Body))
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Auth-Token", c.Token)

	resp, err := client.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Response{}, err
	}

	if resp.StatusCode != r.OkStatusCode {
		return Response{}, fmt.Errorf("%s details: %s\n", resp.Status, body)
	}

	return Response{
		Body:       body,
		StatusCode: resp.StatusCode,
		Headers:    resp.Header}, nil
}
