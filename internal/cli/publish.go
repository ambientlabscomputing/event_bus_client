package cli

import (
	"context"
	"fmt"

	"github.com/ambientlabscomputing/event_bus_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var publishCmd = &cobra.Command{
	Use:   "publish [topic] [content]",
	Short: "Publish a message to a topic",
	Args:  cobra.ExactArgs(2),
	RunE:  runPublish,
}

func init() {
	rootCmd.AddCommand(publishCmd)

	// Add message metadata flags
	publishCmd.Flags().String("trace-id", "", "Trace ID for the message")
	publishCmd.Flags().String("org-id", "", "Organization ID for the message")
	publishCmd.Flags().String("target-type", "", "Target type for the message")
	publishCmd.Flags().String("target-id", "", "Target ID for the message")
}

func runPublish(cmd *cobra.Command, args []string) error {
	topic := args[0]
	content := args[1]

	// Get config (flags override config file)
	endpoint, token, groupID, err := GetConfig()
	if err != nil {
		return err
	}

	opts := event_bus_client.EventClientOpts{
		Endpoint:       endpoint,
		AuthToken:      token,
		CommitInterval: "5s",
		GroupID:        groupID,
	}

	client := event_bus_client.NewEventClient(opts)

	ctx := context.Background()
	if err := client.Connect(ctx, nil); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Get optional metadata flags
	var traceID, orgID, targetType, targetID *string
	if val, _ := cmd.Flags().GetString("trace-id"); val != "" {
		traceID = &val
	}
	if val, _ := cmd.Flags().GetString("org-id"); val != "" {
		orgID = &val
	}
	if val, _ := cmd.Flags().GetString("target-type"); val != "" {
		targetType = &val
	}
	if val, _ := cmd.Flags().GetString("target-id"); val != "" {
		targetID = &val
	}

	ack, err := client.Publish(ctx, topic, content, targetType, targetID, traceID, orgID)
	if err != nil {
		color.Red("✗ Failed to publish: %v", err)
		return fmt.Errorf("failed to publish: %w", err)
	}

	color.Green("✓ Published message to topic '%s'", topic)
	color.White("  Offset: %d", ack.Offset)
	color.White("  Partition: %s", ack.PartitionID)
	return nil
}
