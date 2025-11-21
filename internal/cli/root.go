package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "eventbus_cli",
	Short: "Event bus client CLI tool",
	Long:  `A command line interface for interacting with the event bus`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// If no subcommand is provided, start the shell
		return runShell(cmd, args)
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	// Global flags can be added here
	rootCmd.PersistentFlags().String("endpoint", "", "Event bus endpoint URL")
	rootCmd.PersistentFlags().String("token", "", "Authentication token")
	rootCmd.PersistentFlags().String("group-id", "", "Consumer group ID")

	// Bind flags to viper (flags override config file)
	viper.BindPFlag("endpoint", rootCmd.PersistentFlags().Lookup("endpoint"))
	viper.BindPFlag("token", rootCmd.PersistentFlags().Lookup("token"))
	viper.BindPFlag("group_id", rootCmd.PersistentFlags().Lookup("group-id"))
}

func initConfig() {
	// Get config directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return
	}

	configDir := filepath.Join(homeDir, ".eventbus_cli")
	configPath := filepath.Join(configDir, "config.yaml")

	// Check if config file exists
	if _, err := os.Stat(configPath); err == nil {
		viper.SetConfigFile(configPath)
		if err := viper.ReadInConfig(); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading config file: %v\n", err)
		}
	}
}

// GetConfig returns the configuration values, with flags taking precedence over config file
func GetConfig() (endpoint, token, groupID string, err error) {
	endpoint = viper.GetString("endpoint")
	token = viper.GetString("token")
	groupID = viper.GetString("group_id")

	if endpoint == "" || token == "" || groupID == "" {
		return "", "", "", fmt.Errorf("missing required configuration. Run 'eventbus_cli configure' or provide flags")
	}

	return endpoint, token, groupID, nil
}
