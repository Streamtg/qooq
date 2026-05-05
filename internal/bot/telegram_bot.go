package bot

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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
	callbackListUsers      = "cb_listusers"
	callbackUserAuthAction = "cb_user_auth_action"
	workerAccessURL        = "https://file.streamgramm.workers.dev"
	
	// Telegram CDN DC Endpoints
	telegramDC1 = "https://cdn1.telegram-cdn.org/file/"
	telegramDC2 = "https://cdn2.telegram-cdn.org/file/"
	telegramDC3 = "https://cdn3.telegram-cdn.org/file/"
	telegramDC4 = "https://cdn4.telegram-cdn.org/file/"
	telegramDC5 = "https://cdn5.telegram-cdn.org/file/"
)

// ================================================================
// WorkerPublisher - Publica metadatos al Cloudflare Worker
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
		client:     &http.Client{Timeout: 30 * time.Second},
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

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker responded with status %d: %s", resp.StatusCode, string(respBody))
	}

	wp.logger.Printf("✅ Published to Worker for chat %d", chatID)
	return nil
}

// ================================================================
// TelegramCDNInfo - Información completa para acceso directo al CDN
// ================================================================
type TelegramCDNInfo struct {
	DCID        int    `json:"dc_id"`
	VolumeID    int64  `json:"volume_id"`
	LocalID     int64  `json:"local_id"`
	AccessHash  int64  `json:"access_hash"`
	FileSize    int64  `json:"file_size"`
	MimeType    string `json:"mime_type"`
	FileName    string `json:"file_name"`
	PartSize    int    `json:"part_size"`
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
	httpClient      *http.Client
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
		httpClient:      &http.Client{Timeout: 30 * time.Second},
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
	d := b.tgClient.Dispatcher
	d.AddHandler(handlers.NewCommand("start", b.handleStartCommand))
	d.AddHandler(handlers.NewCommand("authorize", b.handleAuthorizeUser))
	d.AddHandler(handlers.NewCommand("deauthorize", b.handleDeauthorizeUser))
	d.AddHandler(handlers.NewCommand("listusers", b.handleListUsers))
	d.AddHandler(handlers.NewCommand("userinfo", b.handleUserInfo))
	d.AddHandler(handlers.NewCallbackQuery(filters.CallbackQuery.Prefix("cb_"), b.handleCallbackQuery))
	d.AddHandler(handlers.NewAnyUpdate(b.handleAnyUpdate))
	d.AddHandler(handlers.NewMessage(filters.Message.Media, b.handleMediaMessages))
}

func (b *TelegramBot) isWorkerMode() bool {
	return b.config.WorkerBaseURL != ""
}

// ================================================================
// extractTelegramCDNInfo - EXTRAE INFORMACIÓN MTProto DIRECTA
// Esto evita el límite de 20MB de la Bot API
// ================================================================
func (b *TelegramBot) extractTelegramCDNInfo(media tg.MessageMediaClass) (*TelegramCDNInfo, error) {
	var document *tg.Document
	var dcID int

	switch m := media.(type) {
	case *tg.MessageMediaDocument:
		if m.Document == nil {
			return nil, fmt.Errorf("document is nil")
		}
		doc, ok := m.Document.(*tg.Document)
		if !ok {
			return nil, fmt.Errorf("invalid document type")
		}
		document = doc
		dcID = doc.DCID
	case *tg.MessageMediaPhoto:
		if m.Photo == nil {
			return nil, fmt.Errorf("photo is nil")
		}
		photo, ok := m.Photo.(*tg.Photo)
		if !ok {
			return nil, fmt.Errorf("invalid photo type")
		}
		// Para fotos, usar la versión más grande
		var largestSize *tg.PhotoSize
		maxSize := int64(0)
		for _, size := range photo.Sizes {
			if s, ok := size.(*tg.PhotoSize); ok {
				if s.Size > maxSize {
					maxSize = s.Size
					largestSize = s
				}
			}
		}
		if largestSize == nil {
			return nil, fmt.Errorf("no photo size found")
		}
		return &TelegramCDNInfo{
			DCID:       photo.DCID,
			VolumeID:   photo.ID.VolumeID,
			LocalID:    photo.ID.LocalID,
			AccessHash: photo.AccessHash,
			FileSize:   maxSize,
			MimeType:   "image/jpeg",
			FileName:   "photo.jpg",
			PartSize:   512 * 1024,
		}, nil
	case *tg.MessageMediaWebPage:
		return nil, fmt.Errorf("webpage media not supported for direct CDN")
	default:
		return nil, fmt.Errorf("unsupported media type: %T", media)
	}

	// Extraer información del documento
	var fileSize int64
	var mimeType string
	var fileName string

	for _, attr := range document.Attributes {
		switch a := attr.(type) {
		case *tg.DocumentAttributeFilename:
			fileName = a.FileName
		case *tg.DocumentAttributeVideo:
			mimeType = "video/mp4"
		case *tg.DocumentAttributeAudio:
			if a.Voice {
				mimeType = "audio/ogg"
			} else {
				mimeType = "audio/mpeg"
			}
		}
	}

	if fileName == "" {
		fileName = "document"
	}
	if mimeType == "" {
		mimeType = document.MimeType
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
	}

	fileSize = document.Size

	// Obtener información de ubicación del archivo
	var volumeID, localID, accessHash int64
	switch loc := document.Location.(type) {
	case *tg.InputDocumentFileLocation:
		volumeID = loc.VolumeID
		localID = loc.LocalID
		accessHash = loc.AccessHash
	case *tg.InputPhotoFileLocation:
		volumeID = loc.VolumeID
		localID = loc.LocalID
		accessHash = loc.AccessHash
	default:
		return nil, fmt.Errorf("unsupported file location type: %T", document.Location)
	}

	return &TelegramCDNInfo{
		DCID:       dcID,
		VolumeID:   volumeID,
		LocalID:    localID,
		AccessHash: accessHash,
		FileSize:   fileSize,
		MimeType:   mimeType,
		FileName:   fileName,
		PartSize:   512 * 1024, // 512KB chunks
	}, nil
}

// ================================================================
// buildTelegramCDNURL - Construye URL directa del CDN de Telegram
// SIN usar Bot API (sin límite de 20MB)
// ================================================================
func (b *TelegramBot) buildTelegramCDNURL(info *TelegramCDNInfo) string {
	// Codificar información en formato base64 URL-safe
	// El Worker decodificará esto para construir la solicitud MTProto
	data := fmt.Sprintf("%d:%d:%d:%d", info.DCID, info.VolumeID, info.LocalID, info.AccessHash)
	encoded := base64.URLEncoding.EncodeToString([]byte(data))
	
	// URL que el Worker usará para proxy
	return fmt.Sprintf("tgcdn://%s", encoded)
}

// ================================================================
// generateHMACHash - Crea hash HMAC-SHA256 seguro para URLs
// ================================================================
func generateHMACHash(data, secret string, length int) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(data))
	sig := mac.Sum(nil)

	b64 := base64.URLEncoding.EncodeToString(sig)
	b64 = strings.TrimRight(b64, "=")

	if length > 0 && length < len(b64) {
		return b64[:length]
	}
	return b64
}

// ================================================================
// formatFileSize - Convierte bytes a formato legible
// ================================================================
func formatFileSize(sizeBytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case sizeBytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(sizeBytes)/float64(GB))
	case sizeBytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(sizeBytes)/float64(MB))
	case sizeBytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(sizeBytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", sizeBytes)
	}
}

func getFileExtension(fileName, mimeType string) string {
	if idx := strings.LastIndex(fileName, "."); idx != -1 {
		if ext := strings.ToUpper(fileName[idx+1:]); ext != "" {
			return ext
		}
	}
	mimeMap := map[string]string{
		"video/mp4": "MP4", "video/x-matroska": "MKV", "video/webm": "WEBM",
		"video/x-msvideo": "AVI", "video/quicktime": "MOV",
		"audio/mpeg": "MP3", "audio/ogg": "OGG", "audio/wav": "WAV",
		"audio/flac": "FLAC", "audio/aac": "AAC",
		"image/jpeg": "JPG", "image/png": "PNG", "image/gif": "GIF", "image/webp": "WEBP",
		"application/pdf": "PDF",
	}
	if ext, ok := mimeMap[mimeType]; ok {
		return ext
	}
	parts := strings.Split(mimeType, "/")
	if len(parts) == 2 {
		return strings.ToUpper(parts[1])
	}
	return "FILE"
}

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

func buildShareButtons(fileURL, fileName string) []tg.KeyboardButtonRow {
	encodedURL := url.QueryEscape(fileURL)
	encodedText := url.QueryEscape("Check out this file: " + fileName)

	return []tg.KeyboardButtonRow{
		{
			Buttons: []tg.KeyboardButtonClass{
				&tg.KeyboardButtonURL{
					Text: "📱 WhatsApp",
					URL:  fmt.Sprintf("https://wa.me/?text=%s%%20%s", encodedText, encodedURL),
				},
				&tg.KeyboardButtonURL{
					Text: "✈️ Telegram",
					URL:  fmt.Sprintf("https://t.me/share/url?url=%s&text=%s", encodedURL, encodedText),
				},
			},
		},
		{
			Buttons: []tg.KeyboardButtonClass{
				&tg.KeyboardButtonURL{
					Text: "🐦 Twitter/X",
					URL:  fmt.Sprintf("https://twitter.com/intent/tweet?url=%s&text=%s", encodedURL, encodedText),
				},
				&tg.KeyboardButtonURL{
					Text: "📘 Facebook",
					URL:  fmt.Sprintf("https://www.facebook.com/sharer/sharer.php?u=%s", encodedURL),
				},
			},
		},
		{
			Buttons: []tg.KeyboardButtonClass{
				&tg.KeyboardButtonURL{
					Text: "🔗 Open File",
					URL:  fileURL,
				},
			},
		},
	}
}

func buildMediaMessage(file *types.DocumentFile, fileURL string) string {
	emoji := getMediaEmoji(file.MimeType)
	format := getFileExtension(file.FileName, file.MimeType)
	size := formatFileSize(file.FileSize)

	displayName := file.FileName
	if displayName == "" || displayName == "external_media" {
		displayName = "Unnamed file"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s File received successfully\n", emoji))
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━\n")
	sb.WriteString(fmt.Sprintf("📝 Name:   %s\n", displayName))
	sb.WriteString(fmt.Sprintf("📦 Format: %s\n", format))
	sb.WriteString(fmt.Sprintf("⚖️  Size:   %s\n", size))

	if file.Duration > 0 {
		sb.WriteString(fmt.Sprintf("⏱️  Duration: %02d:%02d\n", file.Duration/60, file.Duration%60))
	}
	if file.Width > 0 && file.Height > 0 {
		sb.WriteString(fmt.Sprintf("📐 Resolution: %dx%d\n", file.Width, file.Height))
	}
	if file.Title != "" {
		sb.WriteString(fmt.Sprintf("🎵 Title:  %s\n", file.Title))
	}
	if file.Performer != "" {
		sb.WriteString(fmt.Sprintf("🎤 Artist: %s\n", file.Performer))
	}

	sb.WriteString("━━━━━━━━━━━━━━━━━━━━\n")
	sb.WriteString("🔗 Stream URL:\n")
	sb.WriteString(fmt.Sprintf("<code>%s</code>\n", fileURL))
	sb.WriteString("\n🔗 Share this file:\n")
	return sb.String()
}

// publishMediaToPlayer envía metadatos al Worker/local
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

// ================================================================
// generateFileURL - Genera URL de streaming firmada con HMAC
// ================================================================
func (b *TelegramBot) generateFileURL(messageID int, file *types.DocumentFile, cdnInfo *TelegramCDNInfo) string {
	if b.isWorkerMode() {
		// En modo Worker, usamos el file_id de MTProto
		fileIdStr := strconv.FormatInt(file.ID, 10)
		hash := generateHMACHash(fileIdStr, b.config.HashSecret, 16)
		return fmt.Sprintf("%s/stream/%s/%s", b.config.WorkerBaseURL, fileIdStr, hash)
	}
	
	// Modo local
	hash := utils.GetShortHash(utils.PackFile(
		file.FileName, file.FileSize, file.MimeType, file.ID,
	), b.config.HashLength)
	return fmt.Sprintf("%s/%d/%s", b.config.BaseURL, messageID, hash)
}

// constructWebSocketMessage construye payload para Worker/player
func (b *TelegramBot) constructWebSocketMessage(
	streamURL string,
	file *types.DocumentFile,
	cdnInfo *TelegramCDNInfo,
) map[string]string {
	msg := map[string]string{
		"url":         streamURL,
		"fileName":    file.FileName,
		"fileId":      strconv.FormatInt(file.ID, 10),
		"mimeType":    file.MimeType,
		"fileSize":    strconv.FormatInt(file.FileSize, 10),
		"duration":    strconv.Itoa(file.Duration),
		"width":       strconv.Itoa(file.Width),
		"height":      strconv.Itoa(file.Height),
		"title":       file.Title,
		"performer":   file.Performer,
		"isVoice":     strconv.FormatBool(file.IsVoice),
		"isAnimation": strconv.FormatBool(file.IsAnimation),
	}

	// Información CRÍTICA para que el Worker acceda directamente al CDN
	if cdnInfo != nil {
		msg["dcId"] = strconv.Itoa(cdnInfo.DCID)
		msg["volumeId"] = strconv.FormatInt(cdnInfo.VolumeID, 10)
		msg["localId"] = strconv.FormatInt(cdnInfo.LocalID, 10)
		msg["accessHash"] = strconv.FormatInt(cdnInfo.AccessHash, 10)
		msg["partSize"] = strconv.Itoa(cdnInfo.PartSize)
		msg["useMTProto"] = "true" // Indica al Worker que use MTProto directo
	}

	return msg
}

func (b *TelegramBot) handleStartCommand(ctx *ext.Context, u *ext.Update) error {
	chatID := u.EffectiveChat().GetID()
	user := u.EffectiveUser()

	if user.ID == ctx.Self.ID {
		return nil
	}

	b.logger.Printf("📥 /start from user: %s (ID: %d) in chat: %d", user.FirstName, user.ID, chatID)

	existingUser, err := b.userRepository.GetUserInfo(user.ID)
	if err != nil {
		if err == sql.ErrNoRows {
			existingUser = nil
		} else {
			return fmt.Errorf("failed to retrieve user info: %w", err)
		}
	}

	isFirstUser, err := b.userRepository.IsFirstUser()
	if err != nil {
		return fmt.Errorf("failed to check first user status: %w", err)
	}

	isAdmin, isAuthorized := false, false

	if existingUser == nil {
		if isFirstUser {
			isAuthorized = true
			isAdmin = true
			b.logger.Printf("User %d is the first user, granted admin rights.", user.ID)
		}
		if err = b.userRepository.StoreUserInfo(user.ID, chatID, user.FirstName, user.LastName, user.Username, isAuthorized, isAdmin); err != nil {
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
		"Hello %s, I'm @%s 👋\n\n"+
			"📤 Send or forward any media file (audio, video, photos or documents).\n"+
			"🔗 I'll generate a direct access link for your file.\n\n"+
			"✨ Features:\n"+
			"• Forward media from any chat\n"+
			"• Upload files directly\n"+
			"• Direct link via Cloudflare CDN\n"+
			"• ✅ NO 20MB LIMIT - Uses MTProto directly\n"+
			"• No tunnels, no open ports required",
		user.FirstName, ctx.Self.Username,
	)

	peer := ctx.PeerStorage.GetInputPeerById(chatID)
	req := &tg.MessagesSendMessageRequest{
		Peer:    peer,
		Message: startMsg,
		ReplyMarkup: &tg.ReplyInlineMarkup{
			Rows: []tg.KeyboardButtonRow{{
				Buttons: []tg.KeyboardButtonClass{
					&tg.KeyboardButtonURL{Text: "🔗 Access Service", URL: workerAccessURL},
				},
			}},
		},
	}
	if _, err = ctx.SendMessage(chatID, req); err != nil {
		return fmt.Errorf("failed to send start message: %w", err)
	}

	if !isAuthorized {
		return b.sendReply(ctx, u, "⚠️ You are not authorized yet. Please contact an administrator.")
	}
	return nil
}

func (b *TelegramBot) notifyAdminsAboutNewUser(newUser *tg.User, newUsersChatID int64) {
	admins, err := b.userRepository.GetAllAdmins()
	if err != nil {
		b.logger.Printf("Failed to retrieve admin list: %v", err)
		return
	}

	var msg string
	username, hasUsername := newUser.GetUsername()
	if hasUsername {
		msg = fmt.Sprintf("New user registered: *@%s* (%s %s)\nID: `%d`\n\n_Use the buttons below to manage authorization\\._",
			username, escapeMarkdownV2(newUser.FirstName), escapeMarkdownV2(newUser.LastName), newUser.ID)
	} else {
		msg = fmt.Sprintf("New user registered: %s %s\nID: `%d`\n\n_Use the buttons below to manage authorization\\._",
			escapeMarkdownV2(newUser.FirstName), escapeMarkdownV2(newUser.LastName), newUser.ID)
	}

	markup := &tg.ReplyInlineMarkup{
		Rows: []tg.KeyboardButtonRow{{
			Buttons: []tg.KeyboardButtonClass{
				&tg.KeyboardButtonCallback{
					Text: "✅ Authorize",
					Data: []byte(fmt.Sprintf("%s,%d,authorize", callbackUserAuthAction, newUser.ID)),
				},
				&tg.KeyboardButtonCallback{
					Text: "❌ Decline",
					Data: []byte(fmt.Sprintf("%s,%d,decline", callbackUserAuthAction, newUser.ID)),
				},
			},
		}},
	}

	for _, admin := range admins {
		if admin.UserID == newUser.ID {
			continue
		}
		peer := b.tgCtx.PeerStorage.GetInputPeerById(admin.ChatID)
		req := &tg.MessagesSendMessageRequest{Peer: peer, Message: msg, ReplyMarkup: markup}
		if _, err = b.tgCtx.SendMessage(admin.ChatID, req); err != nil {
			b.logger.Printf("Failed to notify admin %d: %v", admin.UserID, err)
		}
	}
}

func (b *TelegramBot) handleAuthorizeUser(ctx *ext.Context, u *ext.Update) error {
	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
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
	if err = b.userRepository.AuthorizeUser(targetUserID, isAdmin); err != nil {
		return b.sendReply(ctx, u, "Failed to authorize user.")
	}

	suffix := ""
	if isAdmin {
		suffix = " as admin"
	}
	if info, err := b.userRepository.GetUserInfo(targetUserID); err == nil {
		peer := b.tgCtx.PeerStorage.GetInputPeerById(info.ChatID)
		req := &tg.MessagesSendMessageRequest{
			Peer:    peer,
			Message: fmt.Sprintf("✅ You have been authorized%s to use the bot.", suffix),
		}
		_, _ = b.tgCtx.SendMessage(info.ChatID, req)
	}
	return b.sendReply(ctx, u, fmt.Sprintf("✅ User %d authorized%s.", targetUserID, suffix))
}

func (b *TelegramBot) handleDeauthorizeUser(ctx *ext.Context, u *ext.Update) error {
	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
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

	if err = b.userRepository.DeauthorizeUser(targetUserID); err != nil {
		return b.sendReply(ctx, u, "Failed to deauthorize user.")
	}
	if info, err := b.userRepository.GetUserInfo(targetUserID); err == nil {
		peer := b.tgCtx.PeerStorage.GetInputPeerById(info.ChatID)
		req := &tg.MessagesSendMessageRequest{Peer: peer, Message: "❌ You have been deauthorized from using the bot."}
		_, _ = b.tgCtx.SendMessage(info.ChatID, req)
	}
	return b.sendReply(ctx, u, fmt.Sprintf("✅ User %d deauthorized.", targetUserID))
}

func (b *TelegramBot) handleAnyUpdate(ctx *ext.Context, u *ext.Update) error {
	if !b.config.DebugMode {
		return nil
	}
	if u.EffectiveMessage != nil {
		user := u.EffectiveUser()
		b.logger.Debugf("Message from: %s (ID: %d) in chat: %d", user.FirstName, user.ID, u.EffectiveChat().GetID())
	}
	if u.CallbackQuery != nil {
		b.logger.Debugf("Callback from user %d: %s", u.CallbackQuery.UserID, string(u.CallbackQuery.Data))
	}
	return nil
}

func (b *TelegramBot) handleMediaMessages(ctx *ext.Context, u *ext.Update) error {
	chatID := u.EffectiveChat().GetID()
	user := u.EffectiveUser()
	messageID := u.EffectiveMessage.Message.ID

	_, isForwarded := u.EffectiveMessage.Message.GetFwdFrom()
	msgType := "direct"
	if isForwarded {
		msgType = "forwarded"
	}
	b.logger.Printf("📥 Media (%s) from user: %s (ID: %d) in chat: %d", msgType, user.FirstName, user.ID, chatID)

	if !b.isUserChat(ctx, chatID) {
		return dispatcher.EndGroups
	}

	existingUser, err := b.userRepository.GetUserInfo(chatID)
	if err != nil {
		if err == sql.ErrNoRows {
			return b.sendReply(ctx, u, "⚠️ You are not authorized yet. Please contact an administrator.")
		}
		return fmt.Errorf("failed to retrieve user info: %w", err)
	}
	if !existingUser.IsAuthorized {
		return b.sendReply(ctx, u, "⚠️ You are not authorized yet. Please contact an administrator.")
	}

	if b.config.LogChannelID != "" && b.config.LogChannelID != "0" {
		go b.forwardToLogChannel(ctx, u, chatID)
	}

	// ============================================================
	// EXTRAER INFORMACIÓN MTProto DIRECTA (SIN LÍMITE 20MB)
	// ============================================================
	file, err := utils.FileFromMedia(u.EffectiveMessage.Message.Media)
	if err != nil {
		if webPageMedia, ok := u.EffectiveMessage.Message.Media.(*tg.MessageMediaWebPage); ok {
			if _, isEmpty := webPageMedia.Webpage.(*tg.WebPageEmpty); isEmpty {
				if fileURL := utils.ExtractURLFromEntities(u.EffectiveMessage.Message); fileURL != "" {
					mimeType := utils.DetectMimeTypeFromURL(fileURL)
					file = &types.DocumentFile{FileName: "external_media", MimeType: mimeType}
					return b.sendMediaToUser(ctx, u, fileURL, file, nil)
				}
			}
		}
		b.logger.Printf("Error processing media from chat %d: %v", chatID, err)
		return b.sendReply(ctx, u, fmt.Sprintf("❌ Unsupported media type: %v", err))
	}

	// ============================================================
	// OBTENER INFORMACIÓN CDN DE TELEGRAM (MTProto)
	// ============================================================
	var cdnInfo *TelegramCDNInfo
	if b.isWorkerMode() {
		cdnInfo, err = b.extractTelegramCDNInfo(u.EffectiveMessage.Message.Media)
		if err != nil {
			b.logger.Printf("⚠️ Could not extract CDN info for msg %d: %v (will try Bot API fallback)", messageID, err)
			// Continuar sin CDN info - el Worker intentará otro método
		} else {
			b.logger.Printf("✅ Extracted MTProto CDN info for msg %d (DC: %d, Size: %d)", 
				messageID, cdnInfo.DCID, cdnInfo.FileSize)
		}
	}

	// Generar URL de streaming firmada
	streamURL := b.generateFileURL(messageID, file, cdnInfo)
	b.logger.Printf("Generated stream URL for msg %d: %s", messageID, streamURL)

	return b.sendMediaToUser(ctx, u, streamURL, file, cdnInfo)
}

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

	infoMsg := fmt.Sprintf("Media from user:\nID: %d\nName: %s %s\nUsername: %s",
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
		b.logger.Printf("Chat ID %d is not a user type. Stopping.", chatID)
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

// sendMediaToUser envía mensaje de información de medios con botones
func (b *TelegramBot) sendMediaToUser(
	ctx *ext.Context,
	u *ext.Update,
	fileURL string,
	file *types.DocumentFile,
	cdnInfo *TelegramCDNInfo,
) error {
	chatID := u.EffectiveChat().GetID()

	messageText := buildMediaMessage(file, fileURL)
	shareRows := buildShareButtons(fileURL, file.FileName)

	_, err := ctx.Reply(u, ext.ReplyTextString(messageText), &ext.ReplyOpts{
		Markup: &tg.ReplyInlineMarkup{Rows: shareRows},
		ParseMode: &tg.MessageEntityTextURL{},
	})
	if err != nil {
		b.logger.Printf("Error sending reply to chat %d: %v", chatID, err)
		return err
	}

	// Construir y enviar metadatos al Worker/player
	wsMsg := b.constructWebSocketMessage(fileURL, file, cdnInfo)
	b.publishMediaToPlayer(chatID, wsMsg)

	return nil
}

func (b *TelegramBot) handleCallbackQuery(ctx *ext.Context, u *ext.Update) error {
	callbackData := string(u.CallbackQuery.Data)
	if strings.HasPrefix(callbackData, callbackUserAuthAction) {
		return b.handleUserAuthCallback(ctx, u)
	}
	if strings.HasPrefix(callbackData, callbackListUsers) {
		return b.handleListUsersCallback(ctx, u)
	}
	b.logger.Printf("Unknown callback query: %s", callbackData)
	_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{
		QueryID: u.CallbackQuery.QueryID,
		Message: "Unknown action.",
	})
	return nil
}

func (b *TelegramBot) handleUserAuthCallback(ctx *ext.Context, u *ext.Update) error {
	dataParts := strings.Split(string(u.CallbackQuery.Data), ",")
	if len(dataParts) < 3 {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: "Invalid callback data."})
		return nil
	}

	targetUserID, err := strconv.ParseInt(dataParts[1], 10, 64)
	if err != nil {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: "Invalid user ID."})
		return nil
	}
	actionType := dataParts[2]

	adminID := u.EffectiveUser().ID
	adminInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !adminInfo.IsAdmin {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: "Not authorized."})
		return nil
	}

	targetInfo, err := b.userRepository.GetUserInfo(targetUserID)
	if err != nil {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: "Target user not found."})
		return nil
	}

	var adminMsg, userMsg string
	switch actionType {
	case "authorize":
		if targetInfo.IsAuthorized {
			adminMsg = fmt.Sprintf("User %d is already authorized.", targetUserID)
		} else if err = b.userRepository.AuthorizeUser(targetUserID, false); err != nil {
			adminMsg = fmt.Sprintf("Failed to authorize user %d.", targetUserID)
		} else {
			adminMsg = fmt.Sprintf("✅ User %d authorized.", targetUserID)
			userMsg = "✅ You have been authorized to use the bot."
		}
	case "decline":
		if !targetInfo.IsAuthorized {
			adminMsg = fmt.Sprintf("User %d is already deauthorized.", targetUserID)
		} else if err = b.userRepository.DeauthorizeUser(targetUserID); err != nil {
			adminMsg = fmt.Sprintf("Failed to deauthorize user %d.", targetUserID)
		} else {
			adminMsg = fmt.Sprintf("✅ User %d deauthorized.", targetUserID)
			userMsg = "❌ Your request has been declined by an administrator."
		}
	default:
		adminMsg = "Unknown action."
	}

	_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: adminMsg})

	if userMsg != "" {
		peer := b.tgCtx.PeerStorage.GetInputPeerById(targetInfo.ChatID)
		req := &tg.MessagesSendMessageRequest{Peer: peer, Message: userMsg}
		_, _ = b.tgCtx.SendMessage(targetInfo.ChatID, req)
	}
	return nil
}

func (b *TelegramBot) handleListUsersCallback(ctx *ext.Context, u *ext.Update) error {
	dataParts := strings.Split(string(u.CallbackQuery.Data), ",")
	if len(dataParts) < 2 {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: "Invalid pagination data."})
		return nil
	}
	page, err := strconv.Atoi(dataParts[1])
	if err != nil {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: "Invalid page number."})
		return nil
	}
	originalText := u.EffectiveMessage.Text
	u.EffectiveMessage.Text = fmt.Sprintf("/listusers %d", page)
	err = b.handleListUsers(ctx, u)
	u.EffectiveMessage.Text = originalText
	if err != nil {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: "Error loading users."})
	} else {
		_, _ = ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: "List updated."})
	}
	return nil
}

func (b *TelegramBot) handleListUsers(ctx *ext.Context, u *ext.Update) error {
	adminID := u.EffectiveUser().ID
	userInfo, err := b.userRepository.GetUserInfo(adminID)
	if err != nil || !userInfo.IsAdmin {
		return b.sendReply(ctx, u, "You are not authorized to perform this action.")
	}

	const pageSize = 10
	page := 1
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) > 1 {
		if p, err := strconv.Atoi(args[1]); err == nil && p > 0 {
			page = p
		}
	}

	totalUsers, err := b.userRepository.GetUserCount()
	if err != nil {
		return b.sendReply(ctx, u, "Error getting user count.")
	}

	offset := (page - 1) * pageSize
	users, err := b.userRepository.GetAllUsers(offset, pageSize)
	if err != nil {
		return b.sendReply(ctx, u, "Error getting user list.")
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
	msg.WriteString(fmt.Sprintf("\nPage %d of %d (%d total users)", page, totalPages, totalUsers))

	markup := &tg.ReplyInlineMarkup{}
	var buttons []tg.KeyboardButtonClass
	if page > 1 {
		buttons = append(buttons, &tg.KeyboardButtonCallback{
			Text: "⬅️ Previous",
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

	_, err = ctx.Reply(u, ext.ReplyTextString(msg.String()), &ext.ReplyOpts{Markup: markup})
	return err
}

func (b *TelegramBot) handleUserInfo(ctx *ext.Context, u *ext.Update) error {
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

	info, err := b.userRepository.GetUserInfo(targetUserID)
	if err != nil {
		if err == sql.ErrNoRows {
			return b.sendReply(ctx, u, fmt.Sprintf("User with ID %d not found.", targetUserID))
		}
		return b.sendReply(ctx, u, "Error retrieving user information.")
	}

	status := "Not Authorized ❌"
	if info.IsAuthorized {
		status = "Authorized ✅"
	}
	adminStatus := "No 🚫"
	if info.IsAdmin {
		adminStatus = "Yes 👑"
	}
	username := info.Username
	if username == "" {
		username = "N/A"
	}

	msg := fmt.Sprintf(
		"👤 User Details:\nID: %d\nChat ID: %d\nFirst Name: %s\nLast Name: %s\nUsername: @%s\nStatus: %s\nAdmin: %s\nJoined: %s",
		info.UserID, info.ChatID, info.FirstName, info.LastName,
		username, status, adminStatus, info.CreatedAt,
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
