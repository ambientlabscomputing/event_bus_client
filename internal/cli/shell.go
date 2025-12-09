package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/ambientlabscomputing/event_bus_client"
	"github.com/fatih/color"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

var shellCmd = &cobra.Command{
	Use:   "shell",
	Short: "Start an interactive shell session",
	Long:  `Start an interactive shell where you can run multiple commands on a single connection`,
	RunE:  runShell,
}

func init() {
	rootCmd.AddCommand(shellCmd)
}

func getHistoryFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".eventbus_history"
	}
	return fmt.Sprintf("%s/.eventbus_cli/history", home)
}

type Shell struct {
	client        *event_bus_client.Client
	ctx           context.Context
	cancel        context.CancelFunc
	subscriptions map[string]bool
	verbose       bool
	mu            sync.Mutex
}

func runShell(cmd *cobra.Command, args []string) error {
	// Get config
	endpoint, token, groupID, certPath, keyPath, err := GetConfig()
	if err != nil {
		return err
	}

	opts := event_bus_client.EventClientOpts{
		Endpoint:       endpoint,
		AuthToken:      token,
		CommitInterval: "5s",
		GroupID:        groupID,
		CertPath:       certPath,
		KeyPath:        keyPath,
	}

	client, err := event_bus_client.NewEventClient(opts)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shell := &Shell{
		client:        client,
		ctx:           ctx,
		cancel:        cancel,
		subscriptions: make(map[string]bool),
	}

	// Connect to event bus
	color.White("Connecting to event bus...")
	if err := client.Connect(ctx, nil); err != nil {
		color.Red("✗ Failed to connect: %v", err)
		return err
	}
	color.Green("✓ Connected to %s", endpoint)

	// Handle interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		color.Yellow("\nShutting down...")
		cancel()
	}()

	// Start message listeners
	go shell.listenForMessages()
	go shell.listenForRawMessages()

	// Start REPL
	shell.repl()

	return nil
}

func (s *Shell) listenForMessages() {
	incomingMsgChan := s.client.IncomingMsgChannel()
	for {
		select {
		case msg, ok := <-incomingMsgChan:
			if !ok {
				return
			}
			if msg.PartitionID != "" {
				color.Cyan("📨 Message received: Topic=%s, Partition=%s, Offset=%d, Content=%s", msg.Topic, msg.PartitionID, msg.Offset, msg.Content)
			} else {
				color.Cyan("📨 Message received: Topic=%s, Offset=%d, Content=%s", msg.Topic, msg.Offset, msg.Content)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Shell) listenForRawMessages() {
	rawMsgChan := s.client.RawMsgChannel()
	for {
		select {
		case wfMsg, ok := <-rawMsgChan:
			if !ok {
				color.Yellow("Connection closed, stopping raw message listener")
				return
			}
			s.mu.Lock()
			verbose := s.verbose
			s.mu.Unlock()
			if verbose {
				color.Magenta("[VERBOSE] Type=%s, Payload=%+v", wfMsg.MessageType, wfMsg.Payload)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Shell) repl() {
	color.White("\nEventBus Interactive Shell")
	color.White("Commands: subscribe, publish, list, verbose, help, exit")
	color.White("Use ↑/↓ arrows for command history\n")

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	// Load history from file
	historyFile := getHistoryFilePath()
	if f, err := os.Open(historyFile); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		input, err := line.Prompt("eventbus> ")
		if err != nil {
			// Save history before exiting
			if f, err := os.Create(getHistoryFilePath()); err == nil {
				line.WriteHistory(f)
				f.Close()
			}
			if err == liner.ErrPromptAborted {
				color.Yellow("\nGoodbye!")
			} else {
				color.Red("\nError reading input: %v", err)
			}
			s.cancel()
			return
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		line.AppendHistory(input)

		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		command := parts[0]
		args := parts[1:]

		switch command {
		case "subscribe", "sub":
			s.handleSubscribe(args)
		case "publish", "pub":
			s.handlePublish(args)
		case "list", "ls":
			s.handleList()
		case "verbose", "v":
			s.handleVerbose(args)
		case "help", "h":
			s.handleHelp()
		case "exit", "quit", "q":
			// Save history before exiting
			if f, err := os.Create(getHistoryFilePath()); err == nil {
				line.WriteHistory(f)
				f.Close()
			}
			color.Yellow("Goodbye!")
			s.cancel()
			return
		default:
			color.Red("Unknown command: %s (type 'help' for available commands)", command)
		}
	}
}

func (s *Shell) handleSubscribe(args []string) {
	if len(args) < 1 {
		color.Red("Usage: subscribe <topic>")
		return
	}

	topic := args[0]

	s.mu.Lock()
	if s.subscriptions[topic] {
		s.mu.Unlock()
		color.Yellow("Already subscribed to topic '%s'", topic)
		return
	}
	s.mu.Unlock()

	// Get group ID from config
	_, _, groupID, _, _, err := GetConfig()
	if err != nil {
		color.Red("✗ Failed to get config: %v", err)
		return
	}

	subReq := event_bus_client.SubscriptionRequest{
		GroupID: groupID,
		Topic:   topic,
	}

	if err := s.client.Subscribe(s.ctx, subReq); err != nil {
		color.Red("✗ Failed to subscribe: %v", err)
		return
	}

	s.mu.Lock()
	s.subscriptions[topic] = true
	s.mu.Unlock()

	color.Green("✓ Subscribed to topic '%s'", topic)
}

func (s *Shell) handlePublish(args []string) {
	if len(args) < 2 {
		color.Red("Usage: publish <topic> <content>")
		return
	}

	topic := args[0]
	content := strings.Join(args[1:], " ")

	// Remove quotes if present
	content = strings.Trim(content, "\"")

	if s.verbose {
		color.Magenta("Publishing to topic '%s' with content: %s", topic, content)
	}

	ack, err := s.client.Publish(s.ctx, topic, content, nil, nil, nil, nil)
	if err != nil {
		color.Red("✗ Failed to publish: %v", err)
		return
	}

	color.Green("✓ Published to '%s'", topic)
	color.White("  Offset: %d, Partition: %s", ack.Offset, ack.PartitionID)
}

func (s *Shell) handleList() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.subscriptions) == 0 {
		color.White("No active subscriptions")
		return
	}

	color.White("Active subscriptions:")
	for topic := range s.subscriptions {
		color.White("  - %s", topic)
	}
}

func (s *Shell) handleVerbose(args []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(args) == 0 {
		// Toggle verbose mode
		s.verbose = !s.verbose
	} else {
		switch args[0] {
		case "on", "true", "1":
			s.verbose = true
		case "off", "false", "0":
			s.verbose = false
		default:
			color.Red("Usage: verbose [on|off]")
			return
		}
	}

	s.client.SetVerbose(s.verbose)

	if s.verbose {
		color.Green("✓ Verbose mode enabled")
		color.White("  Will show all raw WebSocket messages")
	} else {
		color.Yellow("Verbose mode disabled")
	}
}

func (s *Shell) handleHelp() {
	color.White("\nAvailable commands:")
	color.White("  subscribe <topic>        - Subscribe to a topic (sub)")
	color.White("  publish <topic> <msg>    - Publish a message (pub)")
	color.White("  list                     - List active subscriptions (ls)")
	color.White("  verbose [on|off]         - Toggle verbose debug mode (v)")
	color.White("  help                     - Show this help (h)")
	color.White("  exit                     - Exit the shell (quit, q)")
	fmt.Println()
}
