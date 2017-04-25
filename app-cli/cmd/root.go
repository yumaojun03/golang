// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"golang/app-cli/cmd/common"
	"golang/app-cli/cmd/common/keystone"
)

var (
	user           string
	pass           string
	project        string
	domian         string
	domianProject  string
	authURL        string
	authVersino    string
	uniresEndPoint string
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "app-cli",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	PersistentPreRunE: auth,
}

func auth(cmd *cobra.Command, args []string) error {
	if user == "" || pass == "" || project == "" ||
		domian == "" || domianProject == "" ||
		authURL == "" || authVersino == "" {
		err := `the keystone auth parameter must not be empty, please set them 
in your os environment or pass them through Global Flags!`
		return errors.New(err)
	}

	if uniresEndPoint == "" {
		return errors.New("miss unires service endpoint address")
	}

	client, err := keystone.NewClient(authURL)
	if err != nil {
		return err
	}

	auth := keystone.NewAuth(user, pass, domian)
	token, err := client.GetToken(auth)
	if err != nil {
		return err
	}

	common.GlobalFlag.SetToken(token)
	common.GlobalFlag.SetEndPoint(uniresEndPoint)

	return nil
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVar(&user, "username", os.Getenv("OS_USERNAME"), "keystone auth user")
	RootCmd.PersistentFlags().StringVar(&pass, "password", os.Getenv("OS_PASSWORD"), "keystone auth user password")
	RootCmd.PersistentFlags().StringVar(&project, "project-name", os.Getenv("OS_PROJECT_NAME"), "keystone auth user project name")
	RootCmd.PersistentFlags().StringVar(&domian, "user-domain-name", os.Getenv("OS_USER_DOMAIN_NAME"), "keystone auth user domain name")
	RootCmd.PersistentFlags().StringVar(&domianProject, "project-domain-name", os.Getenv("OS_PROJECT_DOMAIN_NAME"), "keystone auth user project domain")
	RootCmd.PersistentFlags().StringVar(&authURL, "auth_url", os.Getenv("OS_AUTH_URL"), "keyston auth url")
	RootCmd.PersistentFlags().StringVar(&authVersino, "idenntity-api-version", os.Getenv("OS_IDENTITY_API_VERSION"), "keystone auth version")
	RootCmd.PersistentFlags().StringVar(&uniresEndPoint, "api-endpoint", os.Getenv("UNIRES_ENDPOINT"), "unires service endpoint")
}
