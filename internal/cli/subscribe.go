package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/ambientlabscomputing/event_bus_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var subscribeCmd = &cobra.Command{
	Use:   "subscribe [topic]",
	Short: "Subscribe to a topic",
	Args:  cobra.ExactArgs(1),
	RunE:  runSubscribe,
}

func init() {
	rootCmd.AddCommand(subscribeCmd)

	// Add filter flags for subscribing
	subscribeCmd.Flags().String("trace-id", "", "Filter messages by trace ID")
	subscribeCmd.Flags().String("org-id", "", "Filter messages by organization ID")
	subscribeCmd.Flags().String("target-type", "", "Filter messages by target type")
	subscribeCmd.Flags().String("target-id", "", "Filter messages by target ID")
}

func runSubscribe(cmd *cobra.Command, args []string) error {
	topic := args[0]
	color.White(fmt.Sprintf("subscribing to topic %s ...", topic))

	// Get config (flags override config file)
	endpoint, token, groupID, err := GetConfig()
	if err != nil {
		return err
	}
	color.White(fmt.Sprintf("using endpoint: %s", endpoint))

	opts := event_bus_client.EventClientOpts{
		Endpoint:       endpoint,
		AuthToken:      token,
		CommitInterval: "5s",
		GroupID:        groupID,
	}

	client := event_bus_client.NewEventClient(opts)
	color.White("client initialized")

	subs := []event_bus_client.SubscriptionRequest{
		{
			GroupID: groupID,
			Topic:   topic,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	color.White(fmt.Sprintf("connecting to event bus with subscriptions: %v", subs))
	if err := client.Connect(ctx, &subs); err != nil {
		color.Red(fmt.Sprintf("Failed to connect: %v", err))
		return fmt.Errorf("failed to connect: %w", err)
	}
	color.Green("✓ Connected and subscribed successfully!")

	// Handle interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Process messages
	incomingMsgChan := client.IncomingMsgChannel()
	for {
		select {
		case msg := <-incomingMsgChan:
			color.Cyan(fmt.Sprintf("Received message: Topic=%s, Content=%s", msg.Topic, msg.Content))
		case <-ctx.Done():
			return nil
		}
	}
}
