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
	rootCmd.PersistentFlags().StringP("config-file", "c", "", "Path to config file (default: ~/.eventbus_cli/config.yaml)")
	rootCmd.PersistentFlags().String("endpoint", "", "Event bus endpoint URL")
	rootCmd.PersistentFlags().String("token", "", "Authentication token (JWT)")
	rootCmd.PersistentFlags().String("group-id", "", "Consumer group ID")
	// mTLS authentication flags
	rootCmd.PersistentFlags().String("cert", "", "Path to client certificate PEM file (for mTLS)")
	rootCmd.PersistentFlags().String("key", "", "Path to private key PEM file (for mTLS)")

	// Bind flags to viper (flags override config file)
	viper.BindPFlag("config_file", rootCmd.PersistentFlags().Lookup("config-file"))
	viper.BindPFlag("endpoint", rootCmd.PersistentFlags().Lookup("endpoint"))
	viper.BindPFlag("token", rootCmd.PersistentFlags().Lookup("token"))
	viper.BindPFlag("group_id", rootCmd.PersistentFlags().Lookup("group-id"))
	viper.BindPFlag("cert_path", rootCmd.PersistentFlags().Lookup("cert"))
	viper.BindPFlag("key_path", rootCmd.PersistentFlags().Lookup("key"))
}

func initConfig() {
	// Check if a custom config file was specified
	configFile := viper.GetString("config_file")

	if configFile != "" {
		// Use the specified config file
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading config file %s: %v\n", configFile, err)
		}
		return
	}

	// Use default config file location
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
func GetConfig() (endpoint, token, groupID, certPath, keyPath string, err error) {
	endpoint = viper.GetString("endpoint")
	token = viper.GetString("token")
	groupID = viper.GetString("group_id")
	certPath = viper.GetString("cert_path")
	keyPath = viper.GetString("key_path")

	if endpoint == "" || groupID == "" {
		return "", "", "", "", "", fmt.Errorf("missing required configuration: endpoint and group_id are required")
	}

	// Either token or cert/key pair must be provided
	hasMTLS := certPath != "" && keyPath != ""
	hasToken := token != ""

	if !hasMTLS && !hasToken {
		return "", "", "", "", "", fmt.Errorf("authentication required: provide either --token (JWT) or --cert and --key (mTLS)")
	}

	return endpoint, token, groupID, certPath, keyPath, nil
}
