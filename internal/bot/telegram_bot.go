package bot

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"webBridgeBot/internal/config"
	"webBridgeBot/internal/data"
	"webBridgeBot/internal/logger"
	"webBridgeBot/internal/types"
	"webBridgeBot/internal/utils"
	"webBridgeBot/internal/web"

	"github.com/celestix/gotgproto"
	"github.com/celestix/gotgproto/dispatcher"
	"github.com/celestix/gotgproto/dispatcher/handlers"
	"github.com/celestix/gotgproto/dispatcher/handlers/filters"
	"github.com/celestix/gotgproto/ext"
	"github.com/celestix/gotgproto/sessionMaker"
	"github.com/celestix/gotgproto/storage"
	"github.com/glebarez/sqlite"
	"github.com/gotd/td/tg"
)

const (
	callbackResendToPlayer   = "cb_ResendToPlayer"
	callbackPlay             = "cb_Play"
	callbackRestart          = "cb_Restart"
	callbackForward10        = "cb_Fwd10"
	callbackBackward10       = "cb_Bwd10"
	callbackToggleFullscreen = "cb_ToggleFullscreen"
	callbackListUsers        = "cb_listusers"
	callbackUserAuthAction   = "cb_user_auth_action"
)

// ================================================================
// WorkerPublisher - Publishes messages to Cloudflare Worker via HTTP
// The Worker stores metadata in KV, the browser polls for updates.
// NO files are stored - only small JSON metadata (~500 bytes).
// ================================================================
type WorkerPublisher struct {
	workerURL  string
	pushSecret string
	client     *http.Client
	logger     *logger.Logger
}

func newWorkerPublisher(workerURL, pushSecret string, log *logger.Logger) *WorkerPublisher {
	return &WorkerPublisher{
		workerURL:  workerURL,
		pushSecret: pushSecret,
		client:     &http.Client{Timeout: 10 * time.Second},
		logger:     log,
	}
}

// pushMedia sends media metadata to the Worker for the browser player to pick up
func (wp *WorkerPublisher) pushMedia(chatID int64, mediaData map[string]string) error {
	payload := map[string]interface{}{"type": "media"}
	for k, v := range mediaData {
		payload[k] = v
	}
	return wp.push(chatID, payload)
}

// pushControl sends a playback control command to the Worker
func (wp *WorkerPublisher) pushControl(chatID int64, command string, value interface{}) error {
	return wp.push(chatID, map[string]interface{}{
		"type":    "control",
		"command": command,
		"value":   value,
	})
}

func (wp *WorkerPublisher) push(chatID int64, payload map[string]interface{}) error {
	if wp.workerURL == "" {
		return fmt.Errorf("worker URL not configured")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	pushURL := fmt.Sprintf("%s/push/%d", wp.workerURL, chatID)
	req, err := http.NewRequest("POST", pushURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request creation error: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+wp.pushSecret)

	resp, err := wp.client.Do(req)
	if err != nil {
		return fmt.Errorf("push request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker responded with status %d", resp.StatusCode)
	}

	wp.logger.Printf("✅ Published to Worker for chat %d", chatID)
	return nil
}

// ================================================================
// generateHMACHash creates a URL-safe HMAC-SHA256 hash that is
// compatible with the Cloudflare Worker's generateHash() function.
// Both MUST produce identical output for the same input+secret.
// ================================================================
func generateHMACHash(data, secret string, length int) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(data))
	sig := mac.Sum(nil)

	b64 := base64.StdEncoding.EncodeToString(sig)
	b64 = strings.ReplaceAll(b64, "+", "-")
	b64 = strings.ReplaceAll(b64, "/", "_")
	b64 = strings.TrimRight(b64, "=")

	if length > 0 && length < len(b64) {
		return b64[:length]
	}
	return b64
}

// ================================================================
// TelegramBot - Main bot structure
// ================================================================
type TelegramBot struct {
	config          *config.Configuration
	tgClient        *gotgproto.Client
	tgCtx           *ext.Context
	logger          *logger.Logger
	userRepository  *data.UserRepository
	db              *sql.DB
	webServer       *web.Server
	workerPublisher *WorkerPublisher
}

// NewTelegramBot creates a new instance of TelegramBot.
func NewTelegramBot(config *config.Configuration, log *logger.Logger) (*TelegramBot, error) {
	dsn := fmt.Sprintf("file:%s?mode=rwc", config.DatabasePath)
	tgClient, err := gotgproto.NewClient(
		config.ApiID,
		config.ApiHash,
		gotgproto.ClientTypeBot(config.BotToken),
		&gotgproto.ClientOpts{
			InMemory:         true,
			Session:          sessionMaker.SqlSession(sqlite.Open(dsn)),
			DisableCopyright: true,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Telegram client: %w", err)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	userRepository := data.NewUserRepository(db)
	if err := userRepository.InitDB(); err != nil {
		return nil, err
	}

	tgCtx := tgClient.CreateContext()
	webServer := web.NewServer(config, tgClient, tgCtx, log, userRepository)

	// Create the Worker publisher for Cloudflare Worker communication
	wp := newWorkerPublisher(config.WorkerBaseURL, config.PushSecret, log)

	return &TelegramBot{
		config:          config,
		tgClient:        tgClient,
		tgCtx:           tgCtx,
		logger:          log,
		userRepository:  userRepository,
		db:              db,
		webServer:       webServer,
		workerPublisher: wp,
	}, nil
}

// Run starts the Telegram bot and web server.
func (b *TelegramBot) Run() {
	b.logger.Printf("Starting Telegram bot (@%s)...\n", b.tgClient.Self.Username)

	if b.config.WorkerBaseURL != "" {
		b.logger.Printf("☁️  Worker mode: URLs will point to %s", b.config.WorkerBaseURL)
	} else {
		b.logger.Printf("🏠 Local mode: URLs will point to %s", b.config.BaseURL)
	}

	b.registerHandlers()

	go b.webServer.Start()

	if err := b.tgClient.Idle(); err != nil {
		b.logger.Fatalf("Failed to start Telegram client: %s", err)
	}
}

func (b *TelegramBot) registerHandlers() {
	clientDispatcher := b.tgClient.Dispatcher
	clientDispatcher.AddHandler(handlers.NewCommand("start", b.handleStartCommand))
	clientDispatcher.AddHandler(handlers.NewCommand("authorize", b.handleAuthorizeUser))
	clientDispatcher.AddHandler(handlers.NewCommand("deauthorize", b.handleDeauthorizeUser))
	clientDispatcher.AddHandler(handlers.NewCommand("listusers", b.handleListUsers))
	clientDispatcher.AddHandler(handlers.NewCommand("userinfo", b.handleUserInfo))
	clientDispatcher.AddHandler(handlers.NewCallbackQuery(filters.CallbackQuery.Prefix("cb_"), b.handleCallbackQuery))
	clientDispatcher.AddHandler(handlers.NewAnyUpdate(b.handleAnyUpdate))
	clientDispatcher.AddHandler(handlers.NewMessage(filters.Message.Media, b.handleMediaMessages))
}

// isWorkerMode returns true if a Cloudflare Worker URL is configured
func (b *TelegramBot) isWorkerMode() bool {
	return b.config.WorkerBaseURL != ""
}

// getPlayerURL returns the web player URL for a given chat ID
func (b *TelegramBot) getPlayerURL(chatID int64) string {
	if b.isWorkerMode() {
		return fmt.Sprintf("%s/player/%d", b.config.WorkerBaseURL, chatID)
	}
	return fmt.Sprintf("%s/%d", b.config.BaseURL, chatID)
}

// publishToPlayer sends media or control messages to the player.
// Uses Worker if configured, falls back to local WSManager.
func (b *TelegramBot) publishMediaToPlayer(chatID int64, wsMsg map[string]string) {
	if b.isWorkerMode() {
		go func() {
			if err := b.workerPublisher.pushMedia(chatID, wsMsg); err != nil {
				b.logger.Printf("⚠️ Worker push failed for chat %d: %v (falling back to local)", chatID, err)
				b.webServer.GetWSManager().PublishMessage(chatID, wsMsg)
			}
		}()
	} else {
		b.webServer.GetWSManager().PublishMessage(chatID, wsMsg)
	}
}

// publishControlToPlayer sends a control command to the player.
func (b *TelegramBot) publishControlToPlayer(chatID int64, command string, value interface{}) {
	if b.isWorkerMode() {
		go func() {
			if err := b.workerPublisher.pushControl(chatID, command, value); err != nil {
				b.logger.Printf("⚠️ Worker control push failed for chat %d: %v (falling back to local)", chatID, err)
				b.webServer.GetWSManager().PublishControlCommand(chatID, command, value)
			}
		}()
	} else {
		if _, ok := b.webServer.GetWSManager().GetClient(chatID); ok {
			b.webServer.GetWSManager().PublishControlCommand(chatID, command, value)
		}
	}
}

// isPlayerConnected checks if a player is connected (Worker mode always returns true since we use polling)
func (b *TelegramBot) isPlayerConnected(chatID int64) bool {
	if b.isWorkerMode() {
		return true // Worker uses KV+polling, always "connected"
	}
	_, ok := b.webServer.GetWSManager().GetClient(chatID)
	return ok
}

func (b *TelegramBot) handleStartCommand(ctx *ext.Context, u *ext.Update) error {
	chatID := u.EffectiveChat().GetID()
	user := u.EffectiveUser()

	if user.ID == ctx.Self.ID {
		b.logger.Printf("Ignoring /start command from bot's own ID (%d).", user.ID)
		return nil
	}

	b.logger.Printf("📥 Received /start command from user: %s (ID: %d) in chat: %d", user.FirstName, user.ID, chatID)

	if b.config.DebugMode {
		b.logger.Debugf("/start command - User: %s %s, Username: @%s, ChatID: %d",
			user.FirstName, user.LastName, user.Username, chatID)
	}

	existingUser, err := b.userRepository.GetUserInfo(user.ID)
	if err != nil {
		if err == sql.ErrNoRows {
			b.logger.Printf("User %d not found in DB, attempting to register.", user.ID)
			existingUser = nil
		} else {
			b.logger.Printf("Failed to retrieve user info from DB for /start: %v", err)
			return fmt.Errorf("failed to retrieve user info for start command: %w", err)
		}
	}

	isFirstUser, err := b.userRepository.IsFirstUser()
	if err != nil {
		b.logger.Printf("Failed to check if user is first: %v", err)
		return fmt.Errorf("failed to check first user status: %w", err)
	}

	isAdmin := false
	isAuthorized := false

	if existingUser == nil {
		if isFirstUser {
			isAuthorized = true
			isAdmin = true
			b.logger.Printf("User %d is the first user and has been automatically granted admin rights.", user.ID)
		}

		err = b.userRepository.StoreUserInfo(user.ID, chatID, user.FirstName, user.LastName, user.Username, isAuthorized, isAdmin)
		if err != nil {
			b.logger.Printf("Failed to store user info for new user %d: %v", user.ID, err)
			return fmt.Errorf("failed to store user info: %w", err)
		}
		b.logger.Printf("Stored new user %d with isAuthorized=%t, isAdmin=%t", user.ID, isAuthorized, isAdmin)

		if !isAdmin {
			go b.notifyAdminsAboutNewUser(user, chatID)
		}
	} else {
		isAuthorized = existingUser.IsAuthorized
		isAdmin = existingUser.IsAdmin
		b.logger.Printf("User %d already exists in DB with isAuthorized=%t, isAdmin=%t", user.ID, isAuthorized, isAdmin)
	}

	webURL := b.getPlayerURL(chatID)
	startMsg := fmt.Sprintf(
		"Hello %s, I am @%s, your bridge between Telegram and the Web!\n\n"+
			"📤 You can **forward** or **directly upload** media files (audio, video, photos, or documents) to this bot.\n"+
			"🎥 I will instantly generate a streaming link and play it on your web player.\n\n"+
			"✨ **Features:**\n"+
			"• Forward media from any chat\n"+
			"• Upload media directly (including video files as documents)\n"+
			"• Instant web streaming via Cloudflare\n"+
			"• Control playback from Telegram\n"+
			"• No tunnels, no open ports\n\n"+
			"Click 'Open Player' below or access your player here: %s",
		user.FirstName, ctx.Self.Username, webURL,
	)
	err = b.sendMediaURLReply(ctx, u, startMsg, webURL)
	if err != nil {
		b.logger.Printf("Failed to send start message to user %d: %v", user.ID, err)
		return fmt.Errorf("failed to send start message: %w", err)
	}

	if !isAuthorized {
		b.logger.Printf("User %d is NOT authorized. Sending unauthorized message.", user.ID)
		authorizationMsg := "You are not authorized to use this bot yet. Please ask one of the administrators to authorize you and wait until you receive a confirmation."
		return b.sendReply(ctx, u, authorizationMsg)
	}
	return nil
}

func (b *TelegramBot) notifyAdminsAboutNewUser(newUser *tg.User, newUsersChatID int64) {
	admins, err := b.userRepository.GetAllAdmins()
	if err != nil {
		b.logger.Printf("Failed to retrieve admin list: %v", err)
		return
	}

	var notificationMsg string
	username, hasUsername := newUser.GetUsername()
	if hasUsername {
		notificationMsg = fmt.Sprintf("A new user has joined: *@%s* (%s %s)\nID: `%d`\n\n_Use the buttons below to manage authorization\\._", username, escapeMarkdownV2(newUser.FirstName), escapeMarkdownV2(newUser.LastName), newUser.ID)
	} else {
		notificationMsg = fmt.Sprintf("A new user has joined: %s %s\nID: `%d`\n\n_Use the buttons below to manage authorization\\._", escapeMarkdownV2(newUser.FirstName), escapeMarkdownV2(newUser.LastName), newUser.ID)
	}

	markup := &tg.ReplyInlineMarkup{
		Rows: []tg.KeyboardButtonRow{
			{
				Buttons: []tg.KeyboardButtonClass{
					&tg.KeyboardButtonCallback{Text: "✅ Authorize", Data: []byte(fmt.Sprintf("%s,%d,authorize", callbackUserAuthAction, newUser.ID))},
					&tg.KeyboardButtonCallback{Text: "❌ Decline", Data: []byte(fmt.Sprintf("%s,%d,decline", callbackUserAuthAction, newUser.ID))},
				},
			},
		},
	}

	for _, admin := range admins {
		if admin.UserID == newUser.ID && admin.UserID == newUsersChatID {
			continue
		}
		b.logger.Printf("Notifying admin %d about new user %d", admin.UserID, newUser.ID)

		peer := b.tgCtx.PeerStorage.GetInputPeerById(admin.ChatID)
		req := &tg.MessagesSendMessageRequest{
			Peer:        peer,
			Message:     notificationMsg,
			ReplyMarkup: markup,
		}
		_, err = b.tgCtx.SendMessage(admin.ChatID, req)
		if err != nil {
			b.logger.Printf("Failed to notify admin %d: %v", admin.UserID, err)
		}
	}
}

func (b *TelegramBot) handleAuthorizeUser(ctx *ext.Context, u *ext.Update) error {
	b.logger.Printf("📥 Received /authorize command from user ID: %d", u.EffectiveUser().ID)

	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil {
		b.logger.Printf("Failed to retrieve user info for admin check: %v", err)
		return b.sendReply(ctx, u, "Failed to authorize the user.")
	}

	if !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "You are not authorized to perform this action.")
	}

	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.sendReply(ctx, u, "Usage: /authorize <user_id> [admin]")
	}
	targetUserID, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return b.sendReply(ctx, u, "Invalid user ID.")
	}

	isAdmin := len(args) > 2 && args[2] == "admin"

	err = b.userRepository.AuthorizeUser(targetUserID, isAdmin)
	if err != nil {
		b.logger.Printf("Failed to authorize user %d: %v", targetUserID, err)
		return b.sendReply(ctx, u, "Failed to authorize the user.")
	}

	adminMsgSuffix := ""
	if isAdmin {
		adminMsgSuffix = " as an admin"
	}

	targetUserInfo, err := b.userRepository.GetUserInfo(targetUserID)
	if err == nil {
		peer := b.tgCtx.PeerStorage.GetInputPeerById(targetUserInfo.ChatID)
		req := &tg.MessagesSendMessageRequest{
			Peer:    peer,
			Message: fmt.Sprintf("You have been authorized%s to use WebBridgeBot!", adminMsgSuffix),
		}
		_, err = b.tgCtx.SendMessage(targetUserInfo.ChatID, req)
		if err != nil {
			b.logger.Printf("Could not send notification to authorized user %d: %v", targetUserID, err)
		}
	} else {
		b.logger.Printf("Could not get user info for user %d: %v", targetUserID, err)
	}

	return b.sendReply(ctx, u, fmt.Sprintf("User %d has been authorized%s.", targetUserID, adminMsgSuffix))
}

func (b *TelegramBot) handleDeauthorizeUser(ctx *ext.Context, u *ext.Update) error {
	b.logger.Printf("📥 Received /deauthorize command from user ID: %d", u.EffectiveUser().ID)

	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil {
		b.logger.Printf("Failed to retrieve user info for admin check: %v", err)
		return b.sendReply(ctx, u, "Failed to deauthorize the user.")
	}

	if !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "You are not authorized to perform this action.")
	}

	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.sendReply(ctx, u, "Usage: /deauthorize <user_id>")
	}
	targetUserID, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return b.sendReply(ctx, u, "Invalid user ID.")
	}

	err = b.userRepository.DeauthorizeUser(targetUserID)
	if err != nil {
		b.logger.Printf("Failed to deauthorize user %d: %v", targetUserID, err)
		return b.sendReply(ctx, u, "Failed to deauthorize the user.")
	}

	targetUserInfo, err := b.userRepository.GetUserInfo(targetUserID)
	if err == nil {
		peer := b.tgCtx.PeerStorage.GetInputPeerById(targetUserInfo.ChatID)
		req := &tg.MessagesSendMessageRequest{
			Peer:    peer,
			Message: "You have been deauthorized from using WebBridgeBot.",
		}
		_, err = b.tgCtx.SendMessage(targetUserInfo.ChatID, req)
		if err != nil {
			b.logger.Printf("Could not send notification to deauthorized user %d: %v", targetUserID, err)
		}
	} else {
		b.logger.Printf("Could not get user info for user %d: %v", targetUserID, err)
	}

	return b.sendReply(ctx, u, fmt.Sprintf("User %d has been deauthorized.", targetUserID))
}

func (b *TelegramBot) handleAnyUpdate(ctx *ext.Context, u *ext.Update) error {
	if b.config.DebugMode {
		b.logger.Debugf("Received update from user")

		if u.EffectiveMessage != nil {
			user := u.EffectiveUser()
			chatID := u.EffectiveChat().GetID()
			message := u.EffectiveMessage

			b.logger.Debugf("Message from user: %s %s (ID: %d, Username: @%s) in chat: %d",
				user.FirstName, user.LastName, user.ID, user.Username, chatID)
			b.logger.Debugf("Message ID: %d, Date: %d", message.Message.ID, message.Message.Date)

			if fwdFrom, isForwarded := message.Message.GetFwdFrom(); isForwarded {
				b.logger.Debugf("⏩ FORWARDED message - Original date: %d, FromID: %v, FromName: %s",
					fwdFrom.Date, fwdFrom.FromID, fwdFrom.FromName)
			}

			if message.Text != "" {
				textPreview := message.Text
				if len(textPreview) > 100 {
					textPreview = textPreview[:100] + "..."
				}
				b.logger.Debugf("💬 Text message: \"%s\"", textPreview)
			}

			if message.Message.Media != nil {
				mediaType := fmt.Sprintf("%T", message.Message.Media)
				b.logger.Debugf("📎 Media attached - Type: %s", mediaType)

				switch media := message.Message.Media.(type) {
				case *tg.MessageMediaDocument:
					if doc, ok := media.Document.AsNotEmpty(); ok {
						b.logger.Debugf("   Document ID: %d, Size: %d bytes, MimeType: %s",
							doc.ID, doc.Size, doc.MimeType)
					}
				case *tg.MessageMediaPhoto:
					if photo, ok := media.Photo.AsNotEmpty(); ok {
						b.logger.Debugf("   Photo ID: %d, HasStickers: %t",
							photo.ID, photo.HasStickers)
					}
				}
			}

			if replyTo, ok := message.Message.GetReplyTo(); ok {
				if replyMsg, ok := replyTo.(*tg.MessageReplyHeader); ok {
					b.logger.Debugf("💬 Reply to message ID: %d", replyMsg.ReplyToMsgID)
				}
			}

			if markup, ok := message.Message.GetReplyMarkup(); ok {
				b.logger.Debugf("🔘 Message has reply markup: %T", markup)
			}
		}

		if u.CallbackQuery != nil {
			b.logger.Debugf("🔘 Callback query from user %d: %s",
				u.CallbackQuery.UserID, string(u.CallbackQuery.Data))
		}
	}

	return nil
}

func (b *TelegramBot) handleMediaMessages(ctx *ext.Context, u *ext.Update) error {
	chatID := u.EffectiveChat().GetID()
	user := u.EffectiveUser()

	fwdHeader, isForwarded := u.EffectiveMessage.Message.GetFwdFrom()
	messageType := "direct upload"
	if isForwarded {
		messageType = "forwarded message"
		if b.config.DebugMode {
			b.logger.Debugf("Forwarded message detected - Date: %d, FromID: %v, FromName: %s",
				fwdHeader.Date, fwdHeader.FromID, fwdHeader.FromName)
		}
	}

	b.logger.Printf("📥 Received media %s from user: %s (ID: %d) in chat: %d", messageType, user.FirstName, user.ID, chatID)

	if b.config.DebugMode {
		b.logger.Debugf("Message ID: %d, Media Type: %T", u.EffectiveMessage.Message.ID, u.EffectiveMessage.Message.Media)

		if u.EffectiveMessage.Message.Message != "" {
			b.logger.Debugf("Message text length: %d", len(u.EffectiveMessage.Message.Message))
		}
		if len(u.EffectiveMessage.Message.Entities) > 0 {
			b.logger.Debugf("Message has %d entities:", len(u.EffectiveMessage.Message.Entities))
			for i, entity := range u.EffectiveMessage.Message.Entities {
				b.logger.Debugf("  Entity %d: Type=%T, Offset=%d, Length=%d",
					i, entity, entity.GetOffset(), entity.GetLength())
				if urlEntity, ok := entity.(*tg.MessageEntityTextURL); ok {
					b.logger.Debugf("    URL: %s", urlEntity.URL)
				}
			}
		}

		if webPageMedia, ok := u.EffectiveMessage.Message.Media.(*tg.MessageMediaWebPage); ok {
			b.logger.Debugf("MessageMediaWebPage detected - Webpage type: %T", webPageMedia.Webpage)
		}
	}

	if !b.isUserChat(ctx, chatID) {
		return dispatcher.EndGroups
	}

	existingUser, err := b.userRepository.GetUserInfo(chatID)
	if err != nil {
		if err == sql.ErrNoRows {
			b.logger.Printf("User %d not in DB for media message, sending unauthorized message.", chatID)
			if b.config.DebugMode {
				b.logger.Debugf("User %s (ID: %d) not found in database. Message type: %s", user.FirstName, chatID, messageType)
			}
			authorizationMsg := "You are not authorized to use this bot yet. Please ask one of the administrators to authorize you and wait until you receive a confirmation."
			return b.sendReply(ctx, u, authorizationMsg)
		}
		b.logger.Printf("Failed to retrieve user info from DB for media message for user %d: %v", chatID, err)
		return fmt.Errorf("failed to retrieve user info for media handling: %w", err)
	}

	b.logger.Printf("User %d retrieved for media message. isAuthorized=%t, isAdmin=%t", chatID, existingUser.IsAuthorized, existingUser.IsAdmin)

	if b.config.DebugMode {
		b.logger.Debugf("User details - Name: %s %s, Username: %s, ChatID: %d",
			existingUser.FirstName, existingUser.LastName, existingUser.Username, existingUser.ChatID)
	}

	if !existingUser.IsAuthorized {
		b.logger.Printf("User %d is NOT authorized (isAuthorized=%t). Sending unauthorized message for media.", chatID, existingUser.IsAuthorized)
		authorizationMsg := "You are not authorized to use this bot yet. Please ask one of the administrators to authorize you and wait until you receive a confirmation."
		return b.sendReply(ctx, u, authorizationMsg)
	}

	// Forward to log channel if configured
	if b.config.LogChannelID != "" && b.config.LogChannelID != "0" {
		if b.config.DebugMode {
			b.logger.Debugf("Log channel configured: %s. Starting message forwarding in background.", b.config.LogChannelID)
		}
		go func() {
			fromChatID := u.EffectiveChat().GetID()
			messageID := u.EffectiveMessage.Message.ID

			if b.config.DebugMode {
				b.logger.Debugf("Forwarding message %d from chat %d to log channel %s", messageID, fromChatID, b.config.LogChannelID)
			}

			updates, err := utils.ForwardMessages(ctx, fromChatID, b.config.LogChannelID, messageID)
			if err != nil {
				b.logger.Printf("Failed to forward message %d from chat %d to log channel %s: %v", messageID, fromChatID, b.config.LogChannelID, err)
				return
			}

			b.logger.Printf("Successfully forwarded message %d from chat %d to log channel %s", messageID, fromChatID, b.config.LogChannelID)

			var newMsgID int
			for _, update := range updates.GetUpdates() {
				if newMsg, ok := update.(*tg.UpdateNewChannelMessage); ok {
					if m, ok := newMsg.Message.(*tg.Message); ok {
						newMsgID = m.GetID()
						break
					}
				}
			}

			if newMsgID == 0 {
				b.logger.Printf("Could not find new message ID in forward-updates for original msg %d", messageID)
				return
			}

			userInfo, err := b.userRepository.GetUserInfo(fromChatID)
			if err != nil {
				b.logger.Printf("Could not get user info for user %d to send to log channel", fromChatID)
				return
			}

			var usernameDisplay string
			if userInfo.Username != "" {
				usernameDisplay = "@" + userInfo.Username
			} else {
				usernameDisplay = "N/A"
			}

			infoMsg := fmt.Sprintf("Media from user:\nID: %d\nName: %s %s\nUsername: %s",
				userInfo.UserID, userInfo.FirstName, userInfo.LastName, usernameDisplay)

			logChannelPeer, err := utils.GetLogChannelPeer(ctx, b.config.LogChannelID)
			if err != nil {
				b.logger.Printf("Failed to get log channel peer %s to send reply: %v", b.config.LogChannelID, err)
				return
			}

			_, err = ctx.Raw.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer:     logChannelPeer,
				Message:  infoMsg,
				ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: newMsgID},
				RandomID: rand.Int63(),
			})
			if err != nil {
				b.logger.Printf("Failed to send user info to log channel %s as reply: %v", b.config.LogChannelID, err)
			}
		}()
	}

	// Extract file from media
	if b.config.DebugMode {
		b.logger.Debugf("Attempting to extract file information from media for message ID %d", u.EffectiveMessage.Message.ID)
	}

	file, err := utils.FileFromMedia(u.EffectiveMessage.Message.Media)
	if err != nil {
		// Fallback for WebPageEmpty - try to extract URL from entities
		if webPageMedia, ok := u.EffectiveMessage.Message.Media.(*tg.MessageMediaWebPage); ok {
			if _, isEmpty := webPageMedia.Webpage.(*tg.WebPageEmpty); isEmpty {
				fileURL := utils.ExtractURLFromEntities(u.EffectiveMessage.Message)
				if fileURL != "" {
					if b.config.DebugMode {
						b.logger.Debugf("Extracted URL from message entities: %s", fileURL)
					}

					isFileHosting := strings.Contains(strings.ToLower(fileURL), "attach.fahares.com") ||
						strings.Contains(strings.ToLower(fileURL), "filehosting") ||
						strings.Contains(strings.ToLower(fileURL), "upload")

					mimeType := utils.DetectMimeTypeFromURL(fileURL)
					if b.config.DebugMode {
						b.logger.Debugf("Detected MIME type from URL: %s", mimeType)
						if isFileHosting {
							b.logger.Debugf("Warning: URL appears to be a file hosting page")
						}
					}

					file = &types.DocumentFile{
						FileName: "external_media",
						MimeType: mimeType,
						FileSize: 0,
					}

					err := b.sendMediaToUser(ctx, u, fileURL, file, isForwarded)

					if err == nil && isFileHosting {
						warningMsg := "⚠️ Note: This appears to be a file hosting page. If the media doesn't play, please:\n" +
							"• Send the file directly (not forwarded)\n" +
							"• Or provide a direct download link"
						time.Sleep(500 * time.Millisecond)
						_ = b.sendReply(ctx, u, warningMsg)
					}

					return err
				}
			}
		}

		b.logger.Printf("Error processing media message from chat ID %d, message ID %d: %v", chatID, u.EffectiveMessage.Message.ID, err)
		if b.config.DebugMode {
			b.logger.Debugf("Failed to extract file from media type: %T, error: %v", u.EffectiveMessage.Message.Media, err)
		}
		return b.sendReply(ctx, u, fmt.Sprintf("Unsupported media type or error processing file: %v", err))
	}

	if b.config.DebugMode {
		b.logger.Debugf("File extracted successfully - Name: %s, Size: %d bytes, MimeType: %s, ID: %d",
			file.FileName, file.FileSize, file.MimeType, file.ID)
		if file.Width > 0 || file.Height > 0 {
			b.logger.Debugf("Video/Photo dimensions: %dx%d", file.Width, file.Height)
		}
		if file.Duration > 0 {
			b.logger.Debugf("Media duration: %d seconds", file.Duration)
		}
	}

	fileURL := b.generateFileURL(u.EffectiveMessage.Message.ID, file)
	b.logger.Printf("Generated media file URL for message ID %d in chat ID %d: %s (forwarded: %t)", u.EffectiveMessage.Message.ID, chatID, fileURL, isForwarded)

	if b.config.DebugMode {
		b.logger.Debugf("Sending media to user. Message type: %s, FileURL length: %d", messageType, len(fileURL))
	}

	return b.sendMediaToUser(ctx, u, fileURL, file, isForwarded)
}

func (b *TelegramBot) isUserChat(ctx *ext.Context, chatID int64) bool {
	peerChatID := ctx.PeerStorage.GetPeerById(chatID)
	if peerChatID.Type != int(storage.TypeUser) {
		b.logger.Printf("Chat ID %d is not a user type. Terminating processing.", chatID)
		return false
	}
	return true
}

func (b *TelegramBot) sendReply(ctx *ext.Context, u *ext.Update, msg string) error {
	chatID := u.EffectiveChat().GetID()
	peer := ctx.PeerStorage.GetInputPeerById(chatID)

	req := &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  msg,
		Entities: []tg.MessageEntityClass{},
	}

	_, err := ctx.SendMessage(chatID, req)
	if err != nil {
		b.logger.Printf("Failed to send reply to user: %s (ID: %d) - Error: %v", u.EffectiveUser().FirstName, u.EffectiveUser().ID, err)
	}
	return err
}

func (b *TelegramBot) sendMediaURLReply(ctx *ext.Context, u *ext.Update, msg, webURL string) error {
	chatID := u.EffectiveChat().GetID()
	peer := ctx.PeerStorage.GetInputPeerById(chatID)

	entities := b.parseMarkdownToEntities(msg)

	req := &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  b.stripMarkdownSyntax(msg),
		Entities: entities,
		ReplyMarkup: &tg.ReplyInlineMarkup{
			Rows: []tg.KeyboardButtonRow{
				{
					Buttons: []tg.KeyboardButtonClass{
						&tg.KeyboardButtonURL{Text: "🌐 Open Player", URL: webURL},
						&tg.KeyboardButtonURL{Text: "WebBridgeBot on GitHub", URL: "https://github.com/mshafiee/webbridgebot"},
					},
				},
			},
		},
	}

	_, err := ctx.SendMessage(chatID, req)
	if err != nil {
		b.logger.Printf("Failed to send reply to user: %s (ID: %d) - Error: %v", u.EffectiveUser().FirstName, u.EffectiveUser().ID, err)
	}
	return err
}

func (b *TelegramBot) sendMediaToUser(ctx *ext.Context, u *ext.Update, fileURL string, file *types.DocumentFile, isForwarded bool) error {
	messageText := fileURL
	if b.config.DebugMode {
		msgType := "direct upload"
		if isForwarded {
			msgType = "forwarded"
		}
		b.logger.Debugf("Sending %s media to user %d", msgType, u.EffectiveUser().ID)
	}

	// Build keyboard rows
	var keyboardRows []tg.KeyboardButtonRow

	// Row 1: Resend + Stream URL + Open Player
	firstRowButtons := []tg.KeyboardButtonClass{
		&tg.KeyboardButtonCallback{
			Text: "🔄 Resend to Player",
			Data: []byte(fmt.Sprintf("%s,%d", callbackResendToPlayer, u.EffectiveMessage.Message.ID)),
		},
	}

	// Add Stream URL button (Telegram rejects localhost URLs in buttons)
	if !strings.Contains(strings.ToLower(fileURL), "localhost") &&
		!strings.Contains(strings.ToLower(fileURL), "127.0.0.1") {
		firstRowButtons = append(firstRowButtons, &tg.KeyboardButtonURL{Text: "▶️ Stream", URL: fileURL})
	}

	keyboardRows = append(keyboardRows, tg.KeyboardButtonRow{Buttons: firstRowButtons})

	// Row 2: Open Player button (points to Worker player page)
	chatID := u.EffectiveChat().GetID()
	playerURL := b.getPlayerURL(chatID)
	if !strings.Contains(strings.ToLower(playerURL), "localhost") &&
		!strings.Contains(strings.ToLower(playerURL), "127.0.0.1") {
		keyboardRows = append(keyboardRows, tg.KeyboardButtonRow{
			Buttons: []tg.KeyboardButtonClass{
				&tg.KeyboardButtonURL{Text: "🌐 Open Player", URL: playerURL},
			},
		})
	}

	// Row 3: Fullscreen toggle
	keyboardRows = append(keyboardRows, tg.KeyboardButtonRow{
		Buttons: []tg.KeyboardButtonClass{
			&tg.KeyboardButtonCallback{Text: "🖥️ Fullscreen", Data: []byte(callbackToggleFullscreen)},
		},
	})

	// Row 4: Playback controls
	keyboardRows = append(keyboardRows, tg.KeyboardButtonRow{
		Buttons: []tg.KeyboardButtonClass{
			&tg.KeyboardButtonCallback{Text: "⏸️/▶️", Data: []byte(callbackPlay)},
			&tg.KeyboardButtonCallback{Text: "🔄", Data: []byte(callbackRestart)},
			&tg.KeyboardButtonCallback{Text: "⏪ 10s", Data: []byte(callbackBackward10)},
			&tg.KeyboardButtonCallback{Text: "⏩ 10s", Data: []byte(callbackForward10)},
		},
	})

	_, err := ctx.Reply(u, ext.ReplyTextString(messageText), &ext.ReplyOpts{
		Markup: &tg.ReplyInlineMarkup{
			Rows: keyboardRows,
		},
	})
	if err != nil {
		b.logger.Printf("Error sending reply for chat ID %d, message ID %d: %v", chatID, u.EffectiveMessage.Message.ID, err)
		if b.config.DebugMode {
			b.logger.Debugf("Failed to send media message reply: %v", err)
		}
		return err
	}

	if b.config.DebugMode {
		b.logger.Debugf("Reply sent successfully. Publishing media to player for chat ID %d", chatID)
	}

	wsMsg := b.constructWebSocketMessage(fileURL, file)

	if b.config.DebugMode {
		b.logger.Debugf("WebSocket message constructed with %d fields. Publishing to chat ID %d", len(wsMsg), chatID)
	}

	// Publish to player (Worker or local WSManager)
	b.publishMediaToPlayer(chatID, wsMsg)

	if b.config.DebugMode {
		b.logger.Debugf("Media processing completed successfully for message ID %d", u.EffectiveMessage.Message.ID)
	}

	return nil
}

func (b *TelegramBot) constructWebSocketMessage(fileURL string, file *types.DocumentFile) map[string]string {
	// In Worker mode, the URL already points to the Worker, no proxy needed
	finalURL := fileURL
	if !b.isWorkerMode() {
		finalURL = b.wrapWithProxyIfNeeded(fileURL)
		if b.config.DebugMode && finalURL != fileURL {
			b.logger.Debugf("Wrapped external URL with proxy: %s -> %s", fileURL, finalURL)
		}
	}

	return map[string]string{
		"url":         finalURL,
		"fileName":    file.FileName,
		"fileId":      strconv.FormatInt(file.ID, 10),
		"mimeType":    file.MimeType,
		"duration":    strconv.Itoa(file.Duration),
		"width":       strconv.Itoa(file.Width),
		"height":      strconv.Itoa(file.Height),
		"title":       file.Title,
		"performer":   file.Performer,
		"isVoice":     strconv.FormatBool(file.IsVoice),
		"isAnimation": strconv.FormatBool(file.IsAnimation),
	}
}

func (b *TelegramBot) generateFileURL(messageID int, file *types.DocumentFile) string {
	if b.isWorkerMode() {
		// Worker mode: URL points to the Cloudflare Worker /stream/ endpoint
		// The Worker will call Telegram's getFile API and pipe the stream
		fileIdStr := strconv.FormatInt(file.ID, 10)
		hash := generateHMACHash(fileIdStr, b.config.HashSecret, 16)
		return fmt.Sprintf("%s/stream/%s/%s", b.config.WorkerBaseURL, fileIdStr, hash)
	}

	// Local mode: original behavior
	hash := utils.GetShortHash(utils.PackFile(
		file.FileName,
		file.FileSize,
		file.MimeType,
		file.ID,
	), b.config.HashLength)
	return fmt.Sprintf("%s/%d/%s", b.config.BaseURL, messageID, hash)
}

func (b *TelegramBot) handleCallbackQuery(ctx *ext.Context, u *ext.Update) error {
	// Handle user authorization/decline callbacks
	if strings.HasPrefix(string(u.CallbackQuery.Data), callbackUserAuthAction) {
		dataParts := strings.Split(string(u.CallbackQuery.Data), ",")
		if len(dataParts) < 3 {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Invalid user authorization callback data.",
			})
			return nil
		}

		targetUserID, err := strconv.ParseInt(dataParts[1], 10, 64)
		if err != nil {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Invalid user ID in callback data.",
			})
			return nil
		}
		actionType := dataParts[2]

		adminID := u.EffectiveUser().ID
		adminUserInfo, err := b.userRepository.GetUserInfo(adminID)
		if err != nil || !adminUserInfo.IsAdmin {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "You are not authorized to perform this action.",
			})
			return nil
		}

		targetUserInfo, err := b.userRepository.GetUserInfo(targetUserID)
		if err != nil {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Target user not found.",
			})
			return nil
		}

		var adminResponseMessage, userNotificationMessage string

		switch actionType {
		case "authorize":
			if targetUserInfo.IsAuthorized {
				adminResponseMessage = fmt.Sprintf("User %d is already authorized.", targetUserID)
			} else {
				err = b.userRepository.AuthorizeUser(targetUserID, false)
				if err != nil {
					b.logger.Printf("Failed to authorize user %d via callback: %v", targetUserID, err)
					adminResponseMessage = fmt.Sprintf("Failed to authorize user %d.", targetUserID)
				} else {
					adminResponseMessage = fmt.Sprintf("User %d authorized successfully.", targetUserID)
					userNotificationMessage = "You have been authorized to use WebBridgeBot!"
				}
			}
		case "decline":
			if !targetUserInfo.IsAuthorized {
				adminResponseMessage = fmt.Sprintf("User %d is already deauthorized.", targetUserID)
			} else {
				err = b.userRepository.DeauthorizeUser(targetUserID)
				if err != nil {
					b.logger.Printf("Failed to deauthorize user %d via callback: %v", targetUserID, err)
					adminResponseMessage = fmt.Sprintf("Failed to deauthorize user %d.", targetUserID)
				} else {
					adminResponseMessage = fmt.Sprintf("User %d deauthorized successfully.", targetUserID)
					userNotificationMessage = "Your request to use WebBridgeBot has been declined by an administrator."
				}
			}
		default:
			adminResponseMessage = "Unknown authorization action."
		}

		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: adminResponseMessage,
		})

		if userNotificationMessage != "" {
			peer := b.tgCtx.PeerStorage.GetInputPeerById(targetUserInfo.ChatID)
			req := &tg.MessagesSendMessageRequest{
				Peer:    peer,
				Message: userNotificationMessage,
			}
			_, err = b.tgCtx.SendMessage(targetUserInfo.ChatID, req)
			if err != nil {
				b.logger.Printf("Failed to send notification to user %d: %v", targetUserID, err)
			}
		}
		return nil
	}

	// Handle ResendToPlayer callback
	if strings.HasPrefix(string(u.CallbackQuery.Data), callbackResendToPlayer) {
		if b.config.DebugMode {
			b.logger.Debugf("Callback: Processing ResendToPlayer, data: %s", string(u.CallbackQuery.Data))
		}
		dataParts := strings.Split(string(u.CallbackQuery.Data), ",")
		if b.config.DebugMode {
			b.logger.Debugf("Callback: Split into %d parts: %v", len(dataParts), dataParts)
		}
		if len(dataParts) > 1 {
			messageID, err := strconv.Atoi(dataParts[1])
			if err != nil {
				_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
					QueryID: u.CallbackQuery.QueryID,
					Message: "Invalid message ID in callback data.",
				})
				return nil
			}

			file, err := utils.FileFromMessage(ctx, b.tgClient, messageID)
			var fileURL string

			if err != nil {
				// Fallback: Try to extract URL from message entities
				message, msgErr := utils.GetMessage(ctx, b.tgClient, messageID)
				if msgErr == nil && message.Media != nil {
					if webPageMedia, ok := message.Media.(*tg.MessageMediaWebPage); ok {
						if _, isEmpty := webPageMedia.Webpage.(*tg.WebPageEmpty); isEmpty {
							extractedURL := utils.ExtractURLFromEntities(message)
							if extractedURL != "" {
								if b.config.DebugMode {
									b.logger.Debugf("Callback: Extracted URL from entities for message %d: %s", messageID, extractedURL)
								}
								mimeType := utils.DetectMimeTypeFromURL(extractedURL)
								if b.config.DebugMode {
									b.logger.Debugf("Callback: Detected MIME type: %s", mimeType)
								}
								file = &types.DocumentFile{
									FileName: "external_media",
									MimeType: mimeType,
									FileSize: 0,
								}
								fileURL = extractedURL
								if b.config.DebugMode {
									b.logger.Debugf("Callback: Set fileURL to extracted URL, length: %d", len(fileURL))
								}
							}
						}
					}
				}

				if b.config.DebugMode {
					b.logger.Debugf("Callback: After fallback, fileURL length: %d, file is nil: %v", len(fileURL), file == nil)
				}

				if fileURL == "" {
					b.logger.Printf("Error fetching file for message ID %d for callback: %v", messageID, err)
					_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
						QueryID: u.CallbackQuery.QueryID,
						Message: "Failed to retrieve file info.",
					})
					return nil
				}
			} else {
				fileURL = b.generateFileURL(messageID, file)
				if b.config.DebugMode {
					b.logger.Debugf("Callback: Generated file URL: %s", fileURL)
				}
			}

			if b.config.DebugMode {
				b.logger.Debugf("Callback: Constructing message with URL: %s, MIME: %s", fileURL, file.MimeType)
			}

			wsMsg := b.constructWebSocketMessage(fileURL, file)
			chatID := u.EffectiveChat().GetID()
			b.publishMediaToPlayer(chatID, wsMsg)

			if b.config.DebugMode {
				b.logger.Debugf("Callback: Message published successfully")
			}

			successMsg := fmt.Sprintf("The %s file has been sent to the web player.", file.FileName)
			_, err = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				Alert:   false,
				QueryID: u.CallbackQuery.QueryID,
				Message: successMsg,
			})
			if err != nil && b.config.DebugMode {
				b.logger.Debugf("Callback: Error sending answer: %v", err)
			}
			return nil
		} else {
			if b.config.DebugMode {
				b.logger.Debugf("Callback: Invalid data format for ResendToPlayer, parts: %d", len(dataParts))
			}
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Invalid callback data format.",
			})
			return nil
		}
	}

	// Handle simple control callbacks
	callbackType := string(u.CallbackQuery.Data)
	chatID := u.EffectiveChat().GetID()

	switch callbackType {
	case callbackPlay:
		if b.isPlayerConnected(chatID) {
			b.publishControlToPlayer(chatID, "togglePlayPause", nil)
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Playback toggled.",
			})
		} else {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Web player not connected.",
			})
		}

	case callbackRestart:
		if b.isPlayerConnected(chatID) {
			b.publishControlToPlayer(chatID, "restart", nil)
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Restarting media.",
			})
		} else {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Web player not connected.",
			})
		}

	case callbackForward10:
		if b.isPlayerConnected(chatID) {
			b.publishControlToPlayer(chatID, "seek", 10)
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Forwarded 10 seconds.",
			})
		} else {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Web player not connected.",
			})
		}

	case callbackBackward10:
		if b.isPlayerConnected(chatID) {
			b.publishControlToPlayer(chatID, "seek", -10)
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Rewound 10 seconds.",
			})
		} else {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Web player not connected.",
			})
		}

	case callbackToggleFullscreen:
		if b.isPlayerConnected(chatID) {
			b.publishControlToPlayer(chatID, "toggleFullscreen", nil)
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Fullscreen toggled.",
			})
		} else {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Web player not connected.",
			})
		}

	case callbackListUsers:
		dataParts := strings.Split(string(u.CallbackQuery.Data), ",")
		if len(dataParts) < 2 {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Invalid callback data for listusers pagination.",
			})
			return nil
		}
		page, err := strconv.Atoi(dataParts[1])
		if err != nil {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Invalid page number.",
			})
			return nil
		}
		originalMessageText := u.EffectiveMessage.Text
		u.EffectiveMessage.Text = fmt.Sprintf("/listusers %d", page)
		err = b.handleListUsers(ctx, u)
		u.EffectiveMessage.Text = originalMessageText

		if err != nil {
			b.logger.Printf("Error processing listusers callback: %v", err)
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Error loading users.",
			})
		} else {
			_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: u.CallbackQuery.QueryID,
				Message: "Users list updated.",
			})
		}
		return nil

	default:
		b.logger.Printf("Unknown callback query received: %s", u.CallbackQuery.Data)
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "Unknown action.",
		})
		return nil
	}
	return nil
}

// wrapWithProxyIfNeeded wraps external URLs with the proxy endpoint (local mode only)
func (b *TelegramBot) wrapWithProxyIfNeeded(fileURL string) string {
	if strings.HasPrefix(fileURL, "http://") || strings.HasPrefix(fileURL, "https://") {
		if !strings.Contains(fileURL, fmt.Sprintf(":%s", b.config.Port)) &&
			!strings.Contains(fileURL, "localhost") &&
			!strings.HasPrefix(fileURL, b.config.BaseURL) {
			return fmt.Sprintf("/proxy?url=%s", url.QueryEscape(fileURL))
		}
	}
	return fileURL
}

func (b *TelegramBot) handleListUsers(ctx *ext.Context, u *ext.Update) error {
	b.logger.Printf("📥 Received /listusers command from user ID: %d", u.EffectiveUser().ID)

	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "You are not authorized to perform this action.")
	}

	const pageSize = 10
	page := 1
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) > 1 {
		parsedPage, err := strconv.Atoi(args[1])
		if err == nil && parsedPage > 0 {
			page = parsedPage
		}
	}

	totalUsers, err := b.userRepository.GetUserCount()
	if err != nil {
		b.logger.Printf("Failed to get user count: %v", err)
		return b.sendReply(ctx, u, "Error retrieving user count.")
	}

	offset := (page - 1) * pageSize
	users, err := b.userRepository.GetAllUsers(offset, pageSize)
	if err != nil {
		b.logger.Printf("Failed to get users for listing: %v", err)
		return b.sendReply(ctx, u, "Error retrieving user list.")
	}

	if len(users) == 0 {
		return b.sendReply(ctx, u, "No users found or page is empty.")
	}

	var msg strings.Builder
	msg.WriteString("👥 User List\n\n")
	for i, user := range users {
		status := "❌"
		if user.IsAuthorized {
			status = "✅"
		}
		adminStatus := ""
		if user.IsAdmin {
			adminStatus = "👑"
		}
		username := user.Username
		if username == "" {
			username = "N/A"
		}
		msg.WriteString(fmt.Sprintf("%d. ID:%d %s %s (@%s) - Auth: %s Admin: %s\n",
			offset+i+1, user.UserID, user.FirstName, user.LastName, username, status, adminStatus))
	}

	totalPages := (totalUsers + pageSize - 1) / pageSize
	msg.WriteString(fmt.Sprintf("\nPage %d of %d (%d total users)", page, totalPages, totalUsers))

	markup := &tg.ReplyInlineMarkup{}
	var buttons []tg.KeyboardButtonClass
	if page > 1 {
		buttons = append(buttons, &tg.KeyboardButtonCallback{
			Text: "⬅️ Prev",
			Data: []byte(fmt.Sprintf("%s,%d", callbackListUsers, page-1)),
		})
	}
	if page < totalPages {
		buttons = append(buttons, &tg.KeyboardButtonCallback{
			Text: "Next ➡️",
			Data: []byte(fmt.Sprintf("%s,%d", callbackListUsers, page+1)),
		})
	}
	if len(buttons) > 0 {
		markup.Rows = append(markup.Rows, tg.KeyboardButtonRow{Buttons: buttons})
	}

	_, err = ctx.Reply(u, ext.ReplyTextString(msg.String()), &ext.ReplyOpts{
		Markup: markup,
	})
	return err
}

func (b *TelegramBot) handleUserInfo(ctx *ext.Context, u *ext.Update) error {
	b.logger.Printf("📥 Received /userinfo command from user ID: %d", u.EffectiveUser().ID)

	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "You are not authorized to perform this action.")
	}

	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.sendReply(ctx, u, "Usage: /userinfo <user_id>")
	}
	targetUserID, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return b.sendReply(ctx, u, "Invalid user ID.")
	}

	targetUserInfo, err := b.userRepository.GetUserInfo(targetUserID)
	if err != nil {
		if err == sql.ErrNoRows {
			return b.sendReply(ctx, u, fmt.Sprintf("User with ID %d not found.", targetUserID))
		}
		b.logger.Printf("Failed to get user info for ID %d: %v", targetUserID, err)
		return b.sendReply(ctx, u, "Error retrieving user information.")
	}

	status := "Not Authorized ❌"
	if targetUserInfo.IsAuthorized {
		status = "Authorized ✅"
	}
	adminStatus := "No 🚫"
	if targetUserInfo.IsAdmin {
		adminStatus = "Yes 👑"
	}

	username := targetUserInfo.Username
	if username == "" {
		username = "N/A"
	}

	msg := fmt.Sprintf(
		"👤 User Details:\n"+
			"ID: %d\n"+
			"Chat ID: %d\n"+
			"First Name: %s\n"+
			"Last Name: %s\n"+
			"Username: @%s\n"+
			"Status: %s\n"+
			"Admin: %s\n"+
			"Joined: %s",
		targetUserInfo.UserID,
		targetUserInfo.ChatID,
		targetUserInfo.FirstName,
		targetUserInfo.LastName,
		username,
		status,
		adminStatus,
		targetUserInfo.CreatedAt,
	)

	_, err = ctx.Reply(u, ext.ReplyTextString(msg), &ext.ReplyOpts{})
	return err
}

func escapeMarkdownV2(text string) string {
	replacer := strings.NewReplacer(
		"_", "\\_", "*", "\\*", "[", "\\[", "]", "\\]", "(", "\\(", ")", "\\)",
		"~", "\\~", "`", "\\`", ">", "\\>", "#", "\\#", "+", "\\+", "-", "\\-",
		"=", "\\=", "|", "\\|", "{", "\\{", "}", "\\}", ".", "\\.", "!", "\\!",
	)
	return replacer.Replace(text)
}

func (b *TelegramBot) parseMarkdownToEntities(text string) []tg.MessageEntityClass {
	var entities []tg.MessageEntityClass
	strippedOffset := 0
	i := 0

	for i < len(text) {
		if i+1 < len(text) && text[i:i+2] == "**" {
			end := strings.Index(text[i+2:], "**")
			if end != -1 {
				boldText := text[i+2 : i+2+end]
				entities = append(entities, &tg.MessageEntityBold{
					Offset: strippedOffset,
					Length: len(boldText),
				})
				i += 2 + end + 2
				strippedOffset += len(boldText)
				continue
			}
		}
		i++
		strippedOffset++
	}

	return entities
}

func (b *TelegramBot) stripMarkdownSyntax(text string) string {
	result := strings.ReplaceAll(text, "**", "")
	return result
}
