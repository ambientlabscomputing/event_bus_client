package main

import (
	"os"

	"github.com/ambientlabscomputing/event_bus_client/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
