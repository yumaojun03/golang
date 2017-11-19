package main

import (
	"flag"
	"fmt"
	"golang/version-example/version"
)

func main() {
	showVersion := flag.Bool("v", false, "current version")
	flag.Parse()

	if *showVersion == true {
		fmt.Println(version.FullVersion())
		return
	}
}
