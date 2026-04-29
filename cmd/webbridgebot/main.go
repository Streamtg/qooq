package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"webBridgeBot/internal/bot"
	"webBridgeBot/internal/config"
	"webBridgeBot/internal/logger"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// cfg is declared at the package level to allow Cobra to bind flags directly to its fields.
var cfg config.Configuration

// envFilePath stores the custom .env file path if provided
var envFilePath string

func main() {
	// Create an initial logger for startup (will be reconfigured after config loads)
	log := logger.NewDefault("webBridgeBot: ")

	rootCmd := &cobra.Command{
		Use:   "webBridgeBot",
		Short: "WebBridgeBot",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// 1. Initialize Viper: Read from environment variables and .env file.
			// This is called after flags are parsed, so envFilePath is now available.
			config.InitializeViper(log, envFilePath)
			return viper.BindPFlags(cmd.Flags())
		},
		Run: func(cmd *cobra.Command, args []string) {
			// 2. Load final configuration (which now also initializes the cache).
			cfg = config.LoadConfig(log)

			// Update logger level based on loaded configuration
			log.SetLevel(logger.ParseLogLevel(cfg.LogLevel))
			log.Infof("Log level set to: %s", cfg.LogLevel)

			// Log Worker mode status at startup
			if cfg.WorkerBaseURL != "" {
				log.Infof("☁️  Cloudflare Worker mode enabled: %s", cfg.WorkerBaseURL)
			} else {
				log.Infof("🏠 Local mode: serving from %s", cfg.BaseURL)
			}

			// The BinaryCache now has a background worker.
			// We must ensure it's closed properly on shutdown.
			defer func() {
				log.Info("Closing binary cache...")
				if err := cfg.BinaryCache.Close(); err != nil {
					log.Errorf("Error closing binary cache: %v", err)
				}
			}()

			b, err := bot.NewTelegramBot(&cfg, log)
			if err != nil {
				log.Fatalf("Error initializing Telegram bot: %v", err)
			}

			// Setup graceful shutdown
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			// Run the bot in a separate goroutine so we can listen for shutdown signals.
			go func() {
				b.Run()
				// If b.Run() returns (e.g., due to an unrecoverable error),
				// signal the main function to stop.
				stop()
			}()

			log.Info("Bot is running. Press Ctrl+C to exit.")
			<-ctx.Done() // Block here until a signal is received

			log.Info("Shutdown signal received, initiating graceful shutdown...")
			// The deferred cache close will now be executed.
		},
	}

	// ----------------------------------------------------------------
	// Define Cobra flags
	// ----------------------------------------------------------------

	// General
	rootCmd.Flags().StringVar(&envFilePath, "env_file", "",
		"Path to .env file (default: searches in executable directory and current directory)")

	// Telegram credentials (required)
	rootCmd.Flags().IntVar(&cfg.ApiID, "api_id", 0,
		"Telegram API ID (required)")
	rootCmd.Flags().StringVar(&cfg.ApiHash, "api_hash", "",
		"Telegram API Hash (required)")
	rootCmd.Flags().StringVar(&cfg.BotToken, "bot_token", "",
		"Telegram Bot Token (required)")

	// Web server
	rootCmd.Flags().StringVar(&cfg.BaseURL, "base_url", "",
		"Base URL for the web interface (required)")
	rootCmd.Flags().StringVar(&cfg.Port, "port", "8080",
		"Port for the web server (default 8080)")

	// Cloudflare Worker settings (optional - enables Worker mode when set)
	rootCmd.Flags().StringVar(&cfg.WorkerBaseURL, "worker_base_url", "",
		"Cloudflare Worker base URL (e.g. https://file.streamgramm.workers.dev). "+
			"When set, enables Worker mode instead of local streaming.")
	rootCmd.Flags().StringVar(&cfg.PushSecret, "push_secret", "",
		"Secret token for authenticating push requests to the Cloudflare Worker.")
	rootCmd.Flags().StringVar(&cfg.HashSecret, "hash_secret", "",
		"Secret for HMAC hash generation used to sign Worker stream URLs. "+
			"Defaults to api_hash if not set.")

	// File/cache settings
	rootCmd.Flags().IntVar(&cfg.HashLength, "hash_length", 8,
		"Length of the short hash for file URLs (default 8)")
	rootCmd.Flags().StringVar(&cfg.CacheDirectory, "cache_directory", ".cache",
		"Directory to store cached files and database (default .cache)")
	rootCmd.Flags().Int64Var(&cfg.MaxCacheSize, "max_cache_size", 10*1024*1024*1024,
		"Maximum cache size in bytes (default 10GB)")

	// Logging
	rootCmd.Flags().BoolVar(&cfg.DebugMode, "debug_mode", false,
		"Enable debug logging (default false)")
	rootCmd.Flags().StringVar(&cfg.LogLevel, "log_level", "INFO",
		"Log level: DEBUG, INFO, WARNING, ERROR (default INFO)")
	rootCmd.Flags().StringVar(&cfg.LogChannelID, "log_channel_id", "0",
		"Optional: Telegram Channel ID or @username to forward all media to (for logging)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
