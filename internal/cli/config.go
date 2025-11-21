package cli

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configureCmd = &cobra.Command{
	Use:   "configure",
	Short: "Configure the CLI with default settings",
	Long:  `Interactive configuration wizard to set up default endpoint, token, and group-id`,
	RunE:  runConfigure,
}

func init() {
	rootCmd.AddCommand(configureCmd)
}

func runConfigure(cmd *cobra.Command, args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get endpoint
	fmt.Print("Event bus endpoint URL: ")
	endpoint, _ := reader.ReadString('\n')
	endpoint = strings.TrimSpace(endpoint)

	// Get token
	fmt.Print("Authentication token: ")
	token, _ := reader.ReadString('\n')
	token = strings.TrimSpace(token)

	// Get group ID
	fmt.Print("Consumer group ID: ")
	groupID, _ := reader.ReadString('\n')
	groupID = strings.TrimSpace(groupID)

	// Save to config
	viper.Set("endpoint", endpoint)
	viper.Set("token", token)
	viper.Set("group_id", groupID)

	// Get config directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}

	configDir := filepath.Join(homeDir, ".eventbus_cli")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	configPath := filepath.Join(configDir, "config.yaml")
	if err := viper.WriteConfigAs(configPath); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	fmt.Printf("\n✓ Configuration saved to %s\n", configPath)
	fmt.Println("\nYou can now use commands without flags:")
	fmt.Println("  eventbus_cli subscribe my_topic")
	fmt.Println("  eventbus_cli publish my_topic \"hello\"")
	fmt.Println("\nOr override with flags:")
	fmt.Println("  eventbus_cli subscribe my_topic --endpoint ws://other:8080")

	return nil
}
