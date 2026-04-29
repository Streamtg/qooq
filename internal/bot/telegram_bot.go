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
	// Callbacks de administración (se mantienen)
	callbackListUsers      = "cb_listusers"
	callbackUserAuthAction = "cb_user_auth_action"

	// Worker base URL fija
	workerAccessURL = "https://file.streamgramm.workers.dev"
)

// ================================================================
// WorkerPublisher - Publica mensajes al Cloudflare Worker via HTTP
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

func (wp *WorkerPublisher) pushMedia(chatID int64, mediaData map[string]string) error {
	payload := map[string]interface{}{"type": "media"}
	for k, v := range mediaData {
		payload[k] = v
	}
	return wp.push(chatID, payload)
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
// generateHMACHash crea un hash HMAC-SHA256 compatible con el Worker
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
// formatFileSize convierte bytes a formato legible (KB, MB, GB)
// ================================================================
func formatFileSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// getFileExtension extrae la extensión/formato del archivo
func getFileExtension(fileName, mimeType string) string {
	// Intentar desde el nombre del archivo
	if idx := strings.LastIndex(fileName, "."); idx != -1 {
		ext := strings.ToUpper(fileName[idx+1:])
		if ext != "" {
			return ext
		}
	}

	// Fallback desde el MIME type
	mimeMap := map[string]string{
		"video/mp4":        "MP4",
		"video/x-matroska": "MKV",
		"video/webm":       "WEBM",
		"video/avi":        "AVI",
		"video/quicktime":  "MOV",
		"audio/mpeg":       "MP3",
		"audio/ogg":        "OGG",
		"audio/wav":        "WAV",
		"audio/flac":       "FLAC",
		"audio/aac":        "AAC",
		"image/jpeg":       "JPG",
		"image/png":        "PNG",
		"image/gif":        "GIF",
		"image/webp":       "WEBP",
		"application/pdf":  "PDF",
	}

	if ext, ok := mimeMap[mimeType]; ok {
		return ext
	}

	// Extraer tipo genérico del MIME
	parts := strings.Split(mimeType, "/")
	if len(parts) == 2 {
		return strings.ToUpper(parts[1])
	}

	return "ARCHIVO"
}

// getMediaEmoji retorna el emoji apropiado según el tipo de archivo
func getMediaEmoji(mimeType string) string {
	switch {
	case strings.HasPrefix(mimeType, "video/"):
		return "🎬"
	case strings.HasPrefix(mimeType, "audio/"):
		return "🎵"
	case strings.HasPrefix(mimeType, "image/"):
		return "🖼️"
	case mimeType == "application/pdf":
		return "📄"
	default:
		return "📁"
	}
}

// buildShareButtons construye los botones para compartir en redes sociales
func buildShareButtons(fileURL, fileName string) []tg.KeyboardButtonRow {
	encodedURL := url.QueryEscape(fileURL)
	encodedText := url.QueryEscape("¡Mira este archivo: " + fileName)

	shareLinks := []struct {
		name string
		url  string
	}{
		{
			name: "📱 WhatsApp",
			url:  fmt.Sprintf("https://wa.me/?text=%s%%20%s", encodedText, encodedURL),
		},
		{
			name: "✈️ Telegram",
			url:  fmt.Sprintf("https://t.me/share/url?url=%s&text=%s", encodedURL, encodedText),
		},
		{
			name: "🐦 Twitter/X",
			url:  fmt.Sprintf("https://twitter.com/intent/tweet?url=%s&text=%s", encodedURL, encodedText),
		},
		{
			name: "📘 Facebook",
			url:  fmt.Sprintf("https://www.facebook.com/sharer/sharer.php?u=%s", encodedURL),
		},
	}

	var rows []tg.KeyboardButtonRow

	// Fila 1: WhatsApp y Telegram
	rows = append(rows, tg.KeyboardButtonRow{
		Buttons: []tg.KeyboardButtonClass{
			&tg.KeyboardButtonURL{Text: shareLinks[0].name, URL: shareLinks[0].url},
			&tg.KeyboardButtonURL{Text: shareLinks[1].name, URL: shareLinks[1].url},
		},
	})

	// Fila 2: Twitter/X y Facebook
	rows = append(rows, tg.KeyboardButtonRow{
		Buttons: []tg.KeyboardButtonClass{
			&tg.KeyboardButtonURL{Text: shareLinks[2].name, URL: shareLinks[2].url},
			&tg.KeyboardButtonURL{Text: shareLinks[3].name, URL: shareLinks[3].url},
		},
	})

	// Fila 3: Botón de acceso directo al worker
	rows = append(rows, tg.KeyboardButtonRow{
		Buttons: []tg.KeyboardButtonClass{
			&tg.KeyboardButtonURL{
				Text: "🔗 Abrir Archivo",
				URL:  fileURL,
			},
		},
	})

	return rows
}

// buildMediaMessage construye el mensaje informativo del archivo
func buildMediaMessage(file *types.DocumentFile, fileURL string) string {
	emoji := getMediaEmoji(file.MimeType)
	format := getFileExtension(file.FileName, file.MimeType)
	size := formatFileSize(file.FileSize)

	// Nombre limpio del archivo (sin extensión para mostrarlo por separado)
	displayName := file.FileName
	if displayName == "" || displayName == "external_media" {
		displayName = "Archivo sin nombre"
	}

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("%s Archivo recibido correctamente\n", emoji))
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━\n")
	sb.WriteString(fmt.Sprintf("📝 Nombre:  %s\n", displayName))
	sb.WriteString(fmt.Sprintf("📦 Formato: %s\n", format))
	sb.WriteString(fmt.Sprintf("⚖️  Peso:    %s\n", size))

	// Información adicional para video/audio
	if file.Duration > 0 {
		minutes := file.Duration / 60
		seconds := file.Duration % 60
		sb.WriteString(fmt.Sprintf("⏱️  Duración: %02d:%02d\n", minutes, seconds))
	}

	if file.Width > 0 && file.Height > 0 {
		sb.WriteString(fmt.Sprintf("📐 Resolución: %dx%d\n", file.Width, file.Height))
	}

	if file.Title != "" {
		sb.WriteString(fmt.Sprintf("🎵 Título:  %s\n", file.Title))
	}

	if file.Performer != "" {
		sb.WriteString(fmt.Sprintf("🎤 Artista: %s\n", file.Performer))
	}

	sb.WriteString("━━━━━━━━━━━━━━━━━━━━\n")
	sb.WriteString("🔗 Comparte este archivo:\n")

	return sb.String()
}

// ================================================================
// TelegramBot - Estructura principal del bot
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

// NewTelegramBot crea una nueva instancia del bot
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

// Run inicia el bot y el servidor web
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

func (b *TelegramBot) isWorkerMode() bool {
	return b.config.WorkerBaseURL != ""
}

// publishMediaToPlayer envía metadata al player via Worker o WSManager local
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

func (b *TelegramBot) handleStartCommand(ctx *ext.Context, u *ext.Update) error {
	chatID := u.EffectiveChat().GetID()
	user := u.EffectiveUser()

	if user.ID == ctx.Self.ID {
		b.logger.Printf("Ignoring /start command from bot's own ID (%d).", user.ID)
		return nil
	}

	b.logger.Printf("📥 /start from user: %s (ID: %d) in chat: %d", user.FirstName, user.ID, chatID)

	existingUser, err := b.userRepository.GetUserInfo(user.ID)
	if err != nil {
		if err == sql.ErrNoRows {
			existingUser = nil
		} else {
			return fmt.Errorf("failed to retrieve user info for start command: %w", err)
		}
	}

	isFirstUser, err := b.userRepository.IsFirstUser()
	if err != nil {
		return fmt.Errorf("failed to check first user status: %w", err)
	}

	isAdmin := false
	isAuthorized := false

	if existingUser == nil {
		if isFirstUser {
			isAuthorized = true
			isAdmin = true
			b.logger.Printf("User %d is the first user, granted admin rights.", user.ID)
		}

		err = b.userRepository.StoreUserInfo(user.ID, chatID, user.FirstName, user.LastName, user.Username, isAuthorized, isAdmin)
		if err != nil {
			return fmt.Errorf("failed to store user info: %w", err)
		}

		if !isAdmin {
			go b.notifyAdminsAboutNewUser(user, chatID)
		}
	} else {
		isAuthorized = existingUser.IsAuthorized
		isAdmin = existingUser.IsAdmin
	}

	startMsg := fmt.Sprintf(
		"Hola %s, soy @%s 👋\n\n"+
			"📤 Envíame o reenvíame cualquier archivo multimedia (audio, video, fotos o documentos).\n"+
			"🔗 Te generaré un enlace directo de acceso al archivo.\n\n"+
			"✨ Características:\n"+
			"• Reenvía media desde cualquier chat\n"+
			"• Sube archivos directamente\n"+
			"• Enlace directo via Cloudflare\n"+
			"• Sin túneles ni puertos abiertos",
		user.FirstName, ctx.Self.Username,
	)

	// Mensaje de inicio simplificado, solo con enlace al worker
	peer := ctx.PeerStorage.GetInputPeerById(chatID)
	req := &tg.MessagesSendMessageRequest{
		Peer:    peer,
		Message: startMsg,
		ReplyMarkup: &tg.ReplyInlineMarkup{
			Rows: []tg.KeyboardButtonRow{
				{
					Buttons: []tg.KeyboardButtonClass{
						&tg.KeyboardButtonURL{
							Text: "🔗 Acceder al Servicio",
							URL:  workerAccessURL,
						},
					},
				},
			},
		},
	}
	_, err = ctx.SendMessage(chatID, req)
	if err != nil {
		return fmt.Errorf("failed to send start message: %w", err)
	}

	if !isAuthorized {
		authorizationMsg := "⚠️ Aún no estás autorizado para usar este bot. Por favor, contacta a un administrador."
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
		notificationMsg = fmt.Sprintf(
			"Nuevo usuario registrado: *@%s* (%s %s)\nID: `%d`\n\n_Usa los botones para gestionar la autorización\\._",
			username, escapeMarkdownV2(newUser.FirstName), escapeMarkdownV2(newUser.LastName), newUser.ID,
		)
	} else {
		notificationMsg = fmt.Sprintf(
			"Nuevo usuario registrado: %s %s\nID: `%d`\n\n_Usa los botones para gestionar la autorización\\._",
			escapeMarkdownV2(newUser.FirstName), escapeMarkdownV2(newUser.LastName), newUser.ID,
		)
	}

	markup := &tg.ReplyInlineMarkup{
		Rows: []tg.KeyboardButtonRow{
			{
				Buttons: []tg.KeyboardButtonClass{
					&tg.KeyboardButtonCallback{
						Text: "✅ Autorizar",
						Data: []byte(fmt.Sprintf("%s,%d,authorize", callbackUserAuthAction, newUser.ID)),
					},
					&tg.KeyboardButtonCallback{
						Text: "❌ Declinar",
						Data: []byte(fmt.Sprintf("%s,%d,decline", callbackUserAuthAction, newUser.ID)),
					},
				},
			},
		},
	}

	for _, admin := range admins {
		if admin.UserID == newUser.ID {
			continue
		}
		peer := b.tgCtx.PeerStorage.GetInputPeerById(admin.ChatID)
		req := &tg.MessagesSendMessageRequest{
			Peer:        peer,
			Message:     notificationMsg,
			ReplyMarkup: markup,
		}
		if _, err = b.tgCtx.SendMessage(admin.ChatID, req); err != nil {
			b.logger.Printf("Failed to notify admin %d: %v", admin.UserID, err)
		}
	}
}

func (b *TelegramBot) handleAuthorizeUser(ctx *ext.Context, u *ext.Update) error {
	b.logger.Printf("📥 /authorize from user ID: %d", u.EffectiveUser().ID)

	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "No tienes permisos para realizar esta acción.")
	}

	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.sendReply(ctx, u, "Uso: /authorize <user_id> [admin]")
	}

	targetUserID, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return b.sendReply(ctx, u, "ID de usuario inválido.")
	}

	isAdmin := len(args) > 2 && args[2] == "admin"
	if err = b.userRepository.AuthorizeUser(targetUserID, isAdmin); err != nil {
		b.logger.Printf("Failed to authorize user %d: %v", targetUserID, err)
		return b.sendReply(ctx, u, "Error al autorizar al usuario.")
	}

	suffix := ""
	if isAdmin {
		suffix = " como administrador"
	}

	if targetUserInfo, err := b.userRepository.GetUserInfo(targetUserID); err == nil {
		peer := b.tgCtx.PeerStorage.GetInputPeerById(targetUserInfo.ChatID)
		req := &tg.MessagesSendMessageRequest{
			Peer:    peer,
			Message: fmt.Sprintf("✅ Has sido autorizado%s para usar el bot.", suffix),
		}
		if _, err = b.tgCtx.SendMessage(targetUserInfo.ChatID, req); err != nil {
			b.logger.Printf("Could not notify authorized user %d: %v", targetUserID, err)
		}
	}

	return b.sendReply(ctx, u, fmt.Sprintf("✅ Usuario %d autorizado%s correctamente.", targetUserID, suffix))
}

func (b *TelegramBot) handleDeauthorizeUser(ctx *ext.Context, u *ext.Update) error {
	b.logger.Printf("📥 /deauthorize from user ID: %d", u.EffectiveUser().ID)

	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "No tienes permisos para realizar esta acción.")
	}

	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.sendReply(ctx, u, "Uso: /deauthorize <user_id>")
	}

	targetUserID, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return b.sendReply(ctx, u, "ID de usuario inválido.")
	}

	if err = b.userRepository.DeauthorizeUser(targetUserID); err != nil {
		b.logger.Printf("Failed to deauthorize user %d: %v", targetUserID, err)
		return b.sendReply(ctx, u, "Error al desautorizar al usuario.")
	}

	if targetUserInfo, err := b.userRepository.GetUserInfo(targetUserID); err == nil {
		peer := b.tgCtx.PeerStorage.GetInputPeerById(targetUserInfo.ChatID)
		req := &tg.MessagesSendMessageRequest{
			Peer:    peer,
			Message: "❌ Has sido desautorizado para usar el bot.",
		}
		if _, err = b.tgCtx.SendMessage(targetUserInfo.ChatID, req); err != nil {
			b.logger.Printf("Could not notify deauthorized user %d: %v", targetUserID, err)
		}
	}

	return b.sendReply(ctx, u, fmt.Sprintf("✅ Usuario %d desautorizado correctamente.", targetUserID))
}

func (b *TelegramBot) handleAnyUpdate(ctx *ext.Context, u *ext.Update) error {
	if !b.config.DebugMode {
		return nil
	}

	if u.EffectiveMessage != nil {
		user := u.EffectiveUser()
		chatID := u.EffectiveChat().GetID()
		b.logger.Debugf("Message from user: %s (ID: %d) in chat: %d", user.FirstName, user.ID, chatID)
	}

	if u.CallbackQuery != nil {
		b.logger.Debugf("🔘 Callback query from user %d: %s", u.CallbackQuery.UserID, string(u.CallbackQuery.Data))
	}

	return nil
}

func (b *TelegramBot) handleMediaMessages(ctx *ext.Context, u *ext.Update) error {
	chatID := u.EffectiveChat().GetID()
	user := u.EffectiveUser()

	_, isForwarded := u.EffectiveMessage.Message.GetFwdFrom()
	msgType := "directo"
	if isForwarded {
		msgType = "reenviado"
	}

	b.logger.Printf("📥 Media %s de user: %s (ID: %d) en chat: %d", msgType, user.FirstName, user.ID, chatID)

	if !b.isUserChat(ctx, chatID) {
		return dispatcher.EndGroups
	}

	existingUser, err := b.userRepository.GetUserInfo(chatID)
	if err != nil {
		if err == sql.ErrNoRows {
			return b.sendReply(ctx, u, "⚠️ Aún no estás autorizado. Contacta a un administrador.")
		}
		return fmt.Errorf("failed to retrieve user info: %w", err)
	}

	if !existingUser.IsAuthorized {
		return b.sendReply(ctx, u, "⚠️ Aún no estás autorizado. Contacta a un administrador.")
	}

	// Reenviar al canal de log si está configurado
	if b.config.LogChannelID != "" && b.config.LogChannelID != "0" {
		go b.forwardToLogChannel(ctx, u, chatID)
	}

	// Extraer archivo del media
	file, err := utils.FileFromMedia(u.EffectiveMessage.Message.Media)
	if err != nil {
		// Fallback para WebPageEmpty
		if webPageMedia, ok := u.EffectiveMessage.Message.Media.(*tg.MessageMediaWebPage); ok {
			if _, isEmpty := webPageMedia.Webpage.(*tg.WebPageEmpty); isEmpty {
				fileURL := utils.ExtractURLFromEntities(u.EffectiveMessage.Message)
				if fileURL != "" {
					mimeType := utils.DetectMimeTypeFromURL(fileURL)
					file = &types.DocumentFile{
						FileName: "external_media",
						MimeType: mimeType,
						FileSize: 0,
					}
					return b.sendMediaToUser(ctx, u, fileURL, file)
				}
			}
		}

		b.logger.Printf("Error procesando media de chat %d: %v", chatID, err)
		return b.sendReply(ctx, u, fmt.Sprintf("❌ Tipo de media no soportado: %v", err))
	}

	fileURL := b.generateFileURL(u.EffectiveMessage.Message.ID, file)
	b.logger.Printf("URL generada para mensaje %d en chat %d: %s", u.EffectiveMessage.Message.ID, chatID, fileURL)

	return b.sendMediaToUser(ctx, u, fileURL, file)
}

// forwardToLogChannel reenvía el mensaje al canal de log configurado
func (b *TelegramBot) forwardToLogChannel(ctx *ext.Context, u *ext.Update, fromChatID int64) {
	messageID := u.EffectiveMessage.Message.ID

	updates, err := utils.ForwardMessages(ctx, fromChatID, b.config.LogChannelID, messageID)
	if err != nil {
		b.logger.Printf("Failed to forward message %d to log channel: %v", messageID, err)
		return
	}

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
		return
	}

	userInfo, err := b.userRepository.GetUserInfo(fromChatID)
	if err != nil {
		return
	}

	usernameDisplay := "N/A"
	if userInfo.Username != "" {
		usernameDisplay = "@" + userInfo.Username
	}

	infoMsg := fmt.Sprintf("Media de usuario:\nID: %d\nNombre: %s %s\nUsername: %s",
		userInfo.UserID, userInfo.FirstName, userInfo.LastName, usernameDisplay)

	logChannelPeer, err := utils.GetLogChannelPeer(ctx, b.config.LogChannelID)
	if err != nil {
		return
	}

	_, err = ctx.Raw.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:     logChannelPeer,
		Message:  infoMsg,
		ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: newMsgID},
		RandomID: rand.Int63(),
	})
	if err != nil {
		b.logger.Printf("Failed to send info to log channel: %v", err)
	}
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
		b.logger.Printf("Failed to send reply to user %d: %v", u.EffectiveUser().ID, err)
	}
	return err
}

// sendMediaToUser envía el mensaje mejorado con información del archivo y botones de compartir
func (b *TelegramBot) sendMediaToUser(ctx *ext.Context, u *ext.Update, fileURL string, file *types.DocumentFile) error {
	chatID := u.EffectiveChat().GetID()

	if b.config.DebugMode {
		b.logger.Debugf("Enviando media a usuario %d, URL: %s", u.EffectiveUser().ID, fileURL)
	}

	// Construir mensaje informativo
	messageText := buildMediaMessage(file, fileURL)

	// Construir botones de compartir en redes sociales
	shareRows := buildShareButtons(fileURL, file.FileName)

	_, err := ctx.Reply(u, ext.ReplyTextString(messageText), &ext.ReplyOpts{
		Markup: &tg.ReplyInlineMarkup{
			Rows: shareRows,
		},
	})
	if err != nil {
		b.logger.Printf("Error enviando reply al chat %d: %v", chatID, err)
		return err
	}

	// Publicar al player Worker
	wsMsg := b.constructWebSocketMessage(fileURL, file)
	b.publishMediaToPlayer(chatID, wsMsg)

	if b.config.DebugMode {
		b.logger.Debugf("Media procesada correctamente para mensaje %d", u.EffectiveMessage.Message.ID)
	}

	return nil
}

func (b *TelegramBot) constructWebSocketMessage(fileURL string, file *types.DocumentFile) map[string]string {
	finalURL := fileURL
	if !b.isWorkerMode() {
		finalURL = b.wrapWithProxyIfNeeded(fileURL)
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
		fileIdStr := strconv.FormatInt(file.ID, 10)
		hash := generateHMACHash(fileIdStr, b.config.HashSecret, 16)
		return fmt.Sprintf("%s/stream/%s/%s", b.config.WorkerBaseURL, fileIdStr, hash)
	}

	hash := utils.GetShortHash(utils.PackFile(
		file.FileName,
		file.FileSize,
		file.MimeType,
		file.ID,
	), b.config.HashLength)
	return fmt.Sprintf("%s/%d/%s", b.config.BaseURL, messageID, hash)
}

func (b *TelegramBot) handleCallbackQuery(ctx *ext.Context, u *ext.Update) error {
	callbackData := string(u.CallbackQuery.Data)

	// Callbacks de autorización de usuarios
	if strings.HasPrefix(callbackData, callbackUserAuthAction) {
		return b.handleUserAuthCallback(ctx, u)
	}

	// Callback de paginación de usuarios
	if strings.HasPrefix(callbackData, callbackListUsers) {
		return b.handleListUsersCallback(ctx, u)
	}

	// Callback desconocido
	b.logger.Printf("Unknown callback query: %s", callbackData)
	_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
		QueryID: u.CallbackQuery.QueryID,
		Message: "Acción desconocida.",
	})
	return nil
}

// handleUserAuthCallback gestiona los callbacks de autorización
func (b *TelegramBot) handleUserAuthCallback(ctx *ext.Context, u *ext.Update) error {
	dataParts := strings.Split(string(u.CallbackQuery.Data), ",")
	if len(dataParts) < 3 {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "Datos de callback inválidos.",
		})
		return nil
	}

	targetUserID, err := strconv.ParseInt(dataParts[1], 10, 64)
	if err != nil {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "ID de usuario inválido.",
		})
		return nil
	}
	actionType := dataParts[2]

	// Verificar que quien ejecuta es admin
	adminID := u.EffectiveUser().ID
	adminUserInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !adminUserInfo.IsAdmin {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "No tienes permisos para esta acción.",
		})
		return nil
	}

	targetUserInfo, err := b.userRepository.GetUserInfo(targetUserID)
	if err != nil {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "Usuario destino no encontrado.",
		})
		return nil
	}

	var adminMsg, userMsg string

	switch actionType {
	case "authorize":
		if targetUserInfo.IsAuthorized {
			adminMsg = fmt.Sprintf("El usuario %d ya está autorizado.", targetUserID)
		} else {
			if err = b.userRepository.AuthorizeUser(targetUserID, false); err != nil {
				b.logger.Printf("Failed to authorize user %d: %v", targetUserID, err)
				adminMsg = fmt.Sprintf("Error al autorizar usuario %d.", targetUserID)
			} else {
				adminMsg = fmt.Sprintf("✅ Usuario %d autorizado correctamente.", targetUserID)
				userMsg = "✅ Has sido autorizado para usar el bot."
			}
		}
	case "decline":
		if !targetUserInfo.IsAuthorized {
			adminMsg = fmt.Sprintf("El usuario %d ya está desautorizado.", targetUserID)
		} else {
			if err = b.userRepository.DeauthorizeUser(targetUserID); err != nil {
				b.logger.Printf("Failed to deauthorize user %d: %v", targetUserID, err)
				adminMsg = fmt.Sprintf("Error al desautorizar usuario %d.", targetUserID)
			} else {
				adminMsg = fmt.Sprintf("✅ Usuario %d desautorizado correctamente.", targetUserID)
				userMsg = "❌ Tu solicitud ha sido denegada por un administrador."
			}
		}
	default:
		adminMsg = "Acción desconocida."
	}

	_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
		QueryID: u.CallbackQuery.QueryID,
		Message: adminMsg,
	})

	if userMsg != "" {
		peer := b.tgCtx.PeerStorage.GetInputPeerById(targetUserInfo.ChatID)
		req := &tg.MessagesSendMessageRequest{
			Peer:    peer,
			Message: userMsg,
		}
		if _, err = b.tgCtx.SendMessage(targetUserInfo.ChatID, req); err != nil {
			b.logger.Printf("Failed to notify user %d: %v", targetUserID, err)
		}
	}
	return nil
}

// handleListUsersCallback gestiona la paginación de listado de usuarios
func (b *TelegramBot) handleListUsersCallback(ctx *ext.Context, u *ext.Update) error {
	dataParts := strings.Split(string(u.CallbackQuery.Data), ",")
	if len(dataParts) < 2 {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "Datos de paginación inválidos.",
		})
		return nil
	}

	page, err := strconv.Atoi(dataParts[1])
	if err != nil {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "Número de página inválido.",
		})
		return nil
	}

	originalText := u.EffectiveMessage.Text
	u.EffectiveMessage.Text = fmt.Sprintf("/listusers %d", page)
	err = b.handleListUsers(ctx, u)
	u.EffectiveMessage.Text = originalText

	if err != nil {
		b.logger.Printf("Error en listusers callback: %v", err)
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "Error cargando usuarios.",
		})
	} else {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
			QueryID: u.CallbackQuery.QueryID,
			Message: "Lista actualizada.",
		})
	}
	return nil
}

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
	b.logger.Printf("📥 /listusers from user ID: %d", u.EffectiveUser().ID)

	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "No tienes permisos para realizar esta acción.")
	}

	const pageSize = 10
	page := 1
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) > 1 {
		if parsedPage, err := strconv.Atoi(args[1]); err == nil && parsedPage > 0 {
			page = parsedPage
		}
	}

	totalUsers, err := b.userRepository.GetUserCount()
	if err != nil {
		return b.sendReply(ctx, u, "Error obteniendo el conteo de usuarios.")
	}

	offset := (page - 1) * pageSize
	users, err := b.userRepository.GetAllUsers(offset, pageSize)
	if err != nil {
		return b.sendReply(ctx, u, "Error obteniendo la lista de usuarios.")
	}

	if len(users) == 0 {
		return b.sendReply(ctx, u, "No se encontraron usuarios o la página está vacía.")
	}

	var msg strings.Builder
	msg.WriteString("👥 Lista de Usuarios\n\n")
	for i, user := range users {
		status := "❌"
		if user.IsAuthorized {
			status = "✅"
		}
		adminBadge := ""
		if user.IsAdmin {
			adminBadge = "👑"
		}
		username := user.Username
		if username == "" {
			username = "N/A"
		}
		msg.WriteString(fmt.Sprintf("%d. ID:%d %s %s (@%s) Auth:%s %s\n",
			offset+i+1, user.UserID, user.FirstName, user.LastName, username, status, adminBadge))
	}

	totalPages := (totalUsers + pageSize - 1) / pageSize
	msg.WriteString(fmt.Sprintf("\nPágina %d de %d (%d usuarios en total)", page, totalPages, totalUsers))

	markup := &tg.ReplyInlineMarkup{}
	var buttons []tg.KeyboardButtonClass
	if page > 1 {
		buttons = append(buttons, &tg.KeyboardButtonCallback{
			Text: "⬅️ Anterior",
			Data: []byte(fmt.Sprintf("%s,%d", callbackListUsers, page-1)),
		})
	}
	if page < totalPages {
		buttons = append(buttons, &tg.KeyboardButtonCallback{
			Text: "Siguiente ➡️",
			Data: []byte(fmt.Sprintf("%s,%d", callbackListUsers, page+1)),
		})
	}
	if len(buttons) > 0 {
		markup.Rows = append(markup.Rows, tg.KeyboardButtonRow{Buttons: buttons})
	}

	_, err = ctx.Reply(u, ext.ReplyTextString(msg.String()), &ext.ReplyOpts{Markup: markup})
	return err
}

func (b *TelegramBot) handleUserInfo(ctx *ext.Context, u *ext.Update) error {
	b.logger.Printf("📥 /userinfo from user ID: %d", u.EffectiveUser().ID)

	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "No tienes permisos para realizar esta acción.")
	}

	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.sendReply(ctx, u, "Uso: /userinfo <user_id>")
	}

	targetUserID, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return b.sendReply(ctx, u, "ID de usuario inválido.")
	}

	targetUserInfo, err := b.userRepository.GetUserInfo(targetUserID)
	if err != nil {
		if err == sql.ErrNoRows {
			return b.sendReply(ctx, u, fmt.Sprintf("Usuario con ID %d no encontrado.", targetUserID))
		}
		return b.sendReply(ctx, u, "Error obteniendo información del usuario.")
	}

	status := "No Autorizado ❌"
	if targetUserInfo.IsAuthorized {
		status = "Autorizado ✅"
	}
	adminStatus := "No 🚫"
	if targetUserInfo.IsAdmin {
		adminStatus = "Sí 👑"
	}
	username := targetUserInfo.Username
	if username == "" {
		username = "N/A"
	}

	msg := fmt.Sprintf(
		"👤 Detalles del Usuario:\n"+
			"ID: %d\n"+
			"Chat ID: %d\n"+
			"Nombre: %s\n"+
			"Apellido: %s\n"+
			"Username: @%s\n"+
			"Estado: %s\n"+
			"Admin: %s\n"+
			"Registrado: %s",
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
		"_", "\\_", "*", "\\*", "[", "\\[", "]", "\\]",
		"(", "\\(", ")", "\\)", "~", "\\~", "`", "\\`",
		">", "\\>", "#", "\\#", "+", "\\+", "-", "\\-",
		"=", "\\=", "|", "\\|", "{", "\\{", "}", "\\}",
		".", "\\.", "!", "\\!",
	)
	return replacer.Replace(text)
}
