package utils

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"webBridgeBot/internal/cache"
	"webBridgeBot/internal/types"

	"github.com/celestix/gotgproto"
	"github.com/celestix/gotgproto/ext"
	"github.com/celestix/gotgproto/storage"
	"github.com/gotd/td/tg"
)

// Contains checks if a slice contains a specific element.
func Contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

// GetMessage fetches a message by its ID using the Telegram client.
func GetMessage(ctx context.Context, client *gotgproto.Client, messageID int) (*tg.Message, error) {
	messages, err := client.API().MessagesGetMessages(ctx, []tg.InputMessageClass{
		&tg.InputMessageID{ID: messageID},
	})
	if err != nil {
		return nil, err
	}

	if msgs, ok := messages.(*tg.MessagesMessages); ok {
		for _, msg := range msgs.Messages {
			if m, ok := msg.(*tg.Message); ok && m.GetID() == messageID {
				return m, nil
			}
		}
	}

	return nil, fmt.Errorf("message not found")
}

// ExtractURLFromEntities extracts the first URL from message entities.
func ExtractURLFromEntities(msg *tg.Message) string {
	if msg == nil || len(msg.Entities) == 0 {
		return ""
	}

	for _, entity := range msg.Entities {
		switch e := entity.(type) {
		case *tg.MessageEntityTextURL:
			return e.URL
		case *tg.MessageEntityURL:
			offset := e.Offset
			length := e.Length
			if offset >= 0 && offset+length <= len([]rune(msg.Message)) {
				runes := []rune(msg.Message)
				return string(runes[offset : offset+length])
			}
		}
	}

	return ""
}

// DetectMimeTypeFromURL attempts to detect MIME type from URL.
func DetectMimeTypeFromURL(url string) string {
	urlLower := strings.ToLower(url)

	checks := []struct {
		ext  string
		mime string
	}{
		{".mp3", "audio/mpeg"},
		{".m4a", "audio/mp4"},
		{".ogg", "audio/ogg"},
		{".wav", "audio/wav"},
		{".flac", "audio/flac"},
		{".aac", "audio/aac"},
		{".mp4", "video/mp4"},
		{".webm", "video/webm"},
		{".mkv", "video/x-matroska"},
		{".avi", "video/x-msvideo"},
		{".mov", "video/quicktime"},
		{".jpg", "image/jpeg"},
		{".jpeg", "image/jpeg"},
		{".png", "image/png"},
		{".gif", "image/gif"},
		{".webp", "image/webp"},
	}

	for _, c := range checks {
		if strings.Contains(urlLower, c.ext) {
			return c.mime
		}
	}

	return "audio/mpeg"
}

// FileFromMedia extracts file information from various tg.MessageMediaClass types.
func FileFromMedia(media tg.MessageMediaClass) (*types.DocumentFile, error) {
	switch media := media.(type) {
	case *tg.MessageMediaDocument:
		document, ok := media.Document.AsNotEmpty()
		if !ok {
			return nil, fmt.Errorf("document is empty or not a valid type")
		}

		var fileName string
		var videoWidth, videoHeight, videoDuration int
		var audioTitle, audioPerformer string
		var audioDuration int
		var isVoice, isAnimation bool

		for _, attribute := range document.Attributes {
			switch attr := attribute.(type) {
			case *tg.DocumentAttributeFilename:
				fileName = attr.FileName
			case *tg.DocumentAttributeVideo:
				videoWidth = attr.W
				videoHeight = attr.H
				videoDuration = int(attr.Duration)
			case *tg.DocumentAttributeAudio:
				audioDuration = int(attr.Duration)
				audioTitle = attr.Title
				audioPerformer = attr.Performer
				isVoice = attr.Voice
			case *tg.DocumentAttributeAnimated:
				isAnimation = true
			}
		}

		finalDuration := videoDuration
		if finalDuration == 0 {
			finalDuration = audioDuration
		}

		return &types.DocumentFile{
			ID:          document.ID,
			Location:    document.AsInputDocumentFileLocation(),
			FileSize:    document.Size,
			FileName:    fileName,
			MimeType:    document.MimeType,
			Width:       videoWidth,
			Height:      videoHeight,
			Duration:    finalDuration,
			Title:       audioTitle,
			Performer:   audioPerformer,
			IsVoice:     isVoice,
			IsAnimation: isAnimation,
		}, nil

	case *tg.MessageMediaPhoto:
		photo, ok := media.Photo.AsNotEmpty()
		if !ok {
			return nil, fmt.Errorf("photo is empty or not a valid type")
		}

		var largestSize *tg.PhotoSize
		var largestWidth, largestHeight int
		var largestFileSize int64

		for _, size := range photo.GetSizes() {
			if s, ok := size.(*tg.PhotoSize); ok {
				if s.W > largestWidth {
					largestWidth = s.W
					largestHeight = s.H
					largestSize = s
					largestFileSize = int64(s.Size)
				}
			}
		}

		if largestSize == nil {
			return nil, fmt.Errorf("no suitable full-size photo found for streaming")
		}

		photoFileLocation := &tg.InputPhotoFileLocation{
			ID:            photo.ID,
			AccessHash:    photo.AccessHash,
			FileReference: photo.FileReference,
			ThumbSize:     largestSize.GetType(),
		}

		fileName := fmt.Sprintf("photo_%d.jpg", photo.ID)
		mimeType := "image/jpeg"
		switch largestSize.GetType() {
		case "p":
			mimeType = "image/png"
		case "w":
			mimeType = "image/webp"
		case "g":
			mimeType = "image/gif"
		}

		return &types.DocumentFile{
			ID:       photo.ID,
			Location: photoFileLocation,
			FileSize: largestFileSize,
			FileName: fileName,
			MimeType: mimeType,
			Width:    largestWidth,
			Height:   largestHeight,
		}, nil

	case *tg.MessageMediaWebPage:
		webpage, ok := media.Webpage.(*tg.WebPage)
		if !ok {
			switch wp := media.Webpage.(type) {
			case *tg.WebPageEmpty:
				return nil, fmt.Errorf("webpage is empty (WebPageEmpty) - ID: %d", wp.ID)
			case *tg.WebPagePending:
				return nil, fmt.Errorf("webpage is pending (WebPagePending)")
			case *tg.WebPageNotModified:
				return nil, fmt.Errorf("webpage is not modified (WebPageNotModified)")
			default:
				return nil, fmt.Errorf("unexpected webpage type: %T", wp)
			}
		}

		if webpage.Document != nil {
			if doc, ok := webpage.Document.(*tg.Document); ok {
				var fileName string
				var videoWidth, videoHeight, videoDuration int
				var audioTitle, audioPerformer string
				var audioDuration int
				var isVoice, isAnimation bool

				for _, attribute := range doc.Attributes {
					switch attr := attribute.(type) {
					case *tg.DocumentAttributeFilename:
						fileName = attr.FileName
					case *tg.DocumentAttributeVideo:
						videoWidth = attr.W
						videoHeight = attr.H
						videoDuration = int(attr.Duration)
					case *tg.DocumentAttributeAudio:
						audioDuration = int(attr.Duration)
						audioTitle = attr.Title
						audioPerformer = attr.Performer
						isVoice = attr.Voice
					case *tg.DocumentAttributeAnimated:
						isAnimation = true
					}
				}

				if fileName == "" && webpage.Title != "" {
					fileName = webpage.Title
				}

				finalDuration := videoDuration
				if finalDuration == 0 {
					finalDuration = audioDuration
				}

				return &types.DocumentFile{
					ID:          doc.ID,
					Location:    doc.AsInputDocumentFileLocation(),
					FileSize:    doc.Size,
					FileName:    fileName,
					MimeType:    doc.MimeType,
					Width:       videoWidth,
					Height:      videoHeight,
					Duration:    finalDuration,
					Title:       audioTitle,
					Performer:   audioPerformer,
					IsVoice:     isVoice,
					IsAnimation: isAnimation,
				}, nil
			}
		}

		if webpage.Photo != nil {
			if photo, ok := webpage.Photo.(*tg.Photo); ok {
				var largestSize *tg.PhotoSize
				var largestWidth, largestHeight int
				var largestFileSize int64

				for _, size := range photo.GetSizes() {
					if s, ok := size.(*tg.PhotoSize); ok {
						if s.W > largestWidth {
							largestWidth = s.W
							largestHeight = s.H
							largestSize = s
							largestFileSize = int64(s.Size)
						}
					}
				}

				if largestSize == nil {
					return nil, fmt.Errorf("no suitable full-size photo found in webpage")
				}

				photoFileLocation := &tg.InputPhotoFileLocation{
					ID:            photo.ID,
					AccessHash:    photo.AccessHash,
					FileReference: photo.FileReference,
					ThumbSize:     largestSize.GetType(),
				}

				fileName := fmt.Sprintf("webpage_photo_%d.jpg", photo.ID)
				if webpage.Title != "" {
					fileName = webpage.Title + ".jpg"
				}

				mimeType := "image/jpeg"
				switch largestSize.GetType() {
				case "p":
					mimeType = "image/png"
				case "w":
					mimeType = "image/webp"
				case "g":
					mimeType = "image/gif"
				}

				return &types.DocumentFile{
					ID:       photo.ID,
					Location: photoFileLocation,
					FileSize: largestFileSize,
					FileName: fileName,
					MimeType: mimeType,
					Width:    largestWidth,
					Height:   largestHeight,
				}, nil
			}
		}

		return nil, fmt.Errorf("webpage does not contain any extractable media")

	default:
		return nil, fmt.Errorf("unsupported media type: %T", media)
	}
}

// FileFromMessage retrieves file information from a message, using cache if available.
func FileFromMessage(ctx context.Context, client *gotgproto.Client, messageID int) (*types.DocumentFile, error) {
	key := fmt.Sprintf("file:%d:%d", messageID, client.Self.ID)
	var cachedMedia types.DocumentFile
	if err := cache.GetCache().Get(key, &cachedMedia); err == nil {
		return &cachedMedia, nil
	}

	message, err := GetMessage(ctx, client, messageID)
	if err != nil {
		return nil, err
	}

	file, err := FileFromMedia(message.Media)
	if err != nil {
		return nil, err
	}

	_ = cache.GetCache().Set(key, file, 3600)
	return file, nil
}

// ForwardMessages forwards a message from one chat to another.
func ForwardMessages(ctx *ext.Context, fromChatId int64, logChannelIdentifier string, messageID int) (*tg.Updates, error) {
	fromPeer := ctx.PeerStorage.GetInputPeerById(fromChatId)
	if fromPeer.Zero() {
		return nil, fmt.Errorf("fromChatId: %d is not a valid peer", fromChatId)
	}

	toPeer, err := GetLogChannelPeer(ctx, logChannelIdentifier)
	if err != nil {
		return nil, err
	}

	update, err := ctx.Raw.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
		RandomID: []int64{rand.Int63()},
		FromPeer: fromPeer,
		ID:       []int{messageID},
		ToPeer:   toPeer,
	})
	if err != nil {
		return nil, err
	}

	return update.(*tg.Updates), nil
}

// GetLogChannelPeer resolves the log channel peer using the identifier.
func GetLogChannelPeer(ctx *ext.Context, logChannelIdentifier string) (tg.InputPeerClass, error) {
	peer, err := ResolveChannelPeer(ctx, logChannelIdentifier)
	if err != nil {
		return nil, fmt.Errorf("could not resolve log channel peer '%s': %w", logChannelIdentifier, err)
	}
	return peer, nil
}

// ResolveChannelPeer resolves a peer identifier (numeric ID or @username) to a channel peer.
//
// Accepted formats:
//
//	-1001234567890   full negative channel ID  (preferred)
//	1234567890       bare positive channel ID  (auto-converted)
//	@channelname     public username
//	channelname      public username without @
func ResolveChannelPeer(ctx *ext.Context, identifier string) (tg.InputPeerClass, error) {
	identifier = strings.TrimSpace(identifier)

	if identifier == "" || identifier == "0" {
		return nil, fmt.Errorf("empty or zero log channel identifier")
	}

	// Only treat as numeric if the ENTIRE string parses as a valid integer
	if id, err := strconv.ParseInt(identifier, 10, 64); err == nil {
		return resolveChannelByID(ctx, id)
	}

	// Username path
	username := strings.TrimPrefix(identifier, "@")
	return resolveChannelByUsername(ctx, username)
}

// resolveChannelByID resolves a channel from a numeric Telegram ID.
// Handles full negative IDs (-1001234567890) and bare positive IDs (1234567890).
func resolveChannelByID(ctx *ext.Context, id int64) (tg.InputPeerClass, error) {
	idStr := strconv.FormatInt(id, 10)

	var bareID int64

	switch {
	case strings.HasPrefix(idStr, "-100"):
		// Full form: -1001234567890 → bare = 1234567890
		bareStr := strings.TrimPrefix(idStr, "-100")
		var err error
		bareID, err = strconv.ParseInt(bareStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse bare channel ID from '%s': %w", idStr, err)
		}

	case id > 0:
		// Bare positive ID: 1234567890
		bareID = id

	default:
		// Negative but NOT -100 prefix → regular group
		peer := ctx.PeerStorage.GetInputPeerById(id)
		if !peer.Zero() {
			return peer, nil
		}
		return nil, fmt.Errorf("cannot resolve peer with ID %d: not found in peer storage", id)
	}

	// Build full negative ID for peer storage lookup
	fullNegIDStr := "-100" + strconv.FormatInt(bareID, 10)
	fullNegID, _ := strconv.ParseInt(fullNegIDStr, 10, 64)

	// Try peer storage first (has access hash cached)
	peerInfo := ctx.PeerStorage.GetPeerById(fullNegID)
	if peerInfo.Type == int(storage.TypeChannel) {
		resolved, err := ctx.Raw.ChannelsGetChannels(ctx, []tg.InputChannelClass{
			&tg.InputChannel{ChannelID: bareID, AccessHash: peerInfo.AccessHash},
		})
		if err != nil {
			return nil, fmt.Errorf("ChannelsGetChannels failed for channel %d: %w", bareID, err)
		}
		return extractChannelPeer(resolved, bareID)
	}

	// Fallback: try with zero access hash (works if bot is already a member)
	resolved, err := ctx.Raw.ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{ChannelID: bareID, AccessHash: 0},
	})
	if err != nil {
		return nil, fmt.Errorf(
			"could not resolve channel %d: ensure the bot is a member of that channel: %w",
			bareID, err,
		)
	}

	return extractChannelPeer(resolved, bareID)
}

// resolveChannelByUsername resolves a channel from a public @username.
func resolveChannelByUsername(ctx *ext.Context, username string) (tg.InputPeerClass, error) {
	resolved, err := ctx.Raw.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{
		Username: username,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to resolve username '@%s': %w", username, err)
	}

	for _, chat := range resolved.Chats {
		if channel, ok := chat.(*tg.Channel); ok {
			return channel.AsInputPeer(), nil
		}
	}

	return nil, fmt.Errorf("no channel found for username '@%s'", username)
}

// extractChannelPeer finds the matching channel in a ChannelsGetChannels response.
// In gotd/td v0.132.0, ChannelsGetChannels returns MessagesChatsClass.
func extractChannelPeer(resolved tg.MessagesChatsClass, bareID int64) (tg.InputPeerClass, error) {
	var chats []tg.ChatClass

	switch r := resolved.(type) {
	case *tg.MessagesChats:
		chats = r.GetChats()
	case *tg.MessagesChatsSlice:
		chats = r.GetChats()
	default:
		return nil, fmt.Errorf("unexpected type from ChannelsGetChannels: %T", resolved)
	}

	for _, chat := range chats {
		if ch, ok := chat.(*tg.Channel); ok && ch.GetID() == bareID {
			return ch.AsInputPeer(), nil
		}
	}

	return nil, fmt.Errorf("channel ID %d not found in ChannelsGetChannels response", bareID)
}

// ExtractFloodWait checks if an error is a FLOOD_WAIT error and extracts the wait time.
func ExtractFloodWait(err error) (int, bool) {
	if err == nil {
		return 0, false
	}

	errText := err.Error()
	if !strings.Contains(errText, "FLOOD_WAIT") {
		return 0, false
	}

	start := strings.Index(errText, "FLOOD_WAIT")
	if start < 0 {
		return 5, true
	}

	remaining := errText[start:]

	// Pattern: FLOOD_WAIT (123)
	if i := strings.Index(remaining, "("); i >= 0 {
		if j := strings.Index(remaining[i:], ")"); j >= 0 {
			numStr := strings.TrimSpace(remaining[i+1 : i+j])
			if waitTime, err := strconv.Atoi(numStr); err == nil {
				return waitTime, true
			}
		}
	}

	// Pattern: FLOOD_WAIT_123
	parts := strings.Split(remaining, "_")
	for _, part := range parts {
		if waitTime, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
			return waitTime, true
		}
	}

	return 5, true
}
