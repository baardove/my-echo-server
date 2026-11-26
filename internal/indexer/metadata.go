package indexer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"golang.org/x/text/unicode/norm"
)

var recognizedExtensions = map[string]string{
	".flac": "flac",
	".mp3":  "mp3",
	".m4a":  "m4a",
	".aac":  "m4a",
	".mp4":  "m4a",
	".alac": "alac",
	".wav":  "wav",
	".wave": "wav",
	".ogg":  "ogg",
	".oga":  "ogg",
	".ape":  "ape",
	".dsf":  "dsf",
	".dff":  "dff",
	".aif":  "aiff",
	".aiff": "aiff",
	".opus": "opus",
	".wma":  "wma",
}

func RecognizeFormat(ext string) (string, bool) {
	f, ok := recognizedExtensions[strings.ToLower(ext)]
	return f, ok
}

// StableTrackID matches Swift AudioMetadataReader.stableTrackID(forPath:):
// SHA256(NFC(path))[:16] lowercase hex.
func StableTrackID(path string) string {
	nfc := norm.NFC.String(path)
	sum := sha256.Sum256([]byte(nfc))
	return hex.EncodeToString(sum[:16])
}

// TrackKey hashes (libraryID + ":" + NFC(relativePath))[:16].
func TrackKey(libraryID, relPath string) string {
	nfc := norm.NFC.String(relPath)
	sum := sha256.Sum256([]byte(libraryID + ":" + nfc))
	return hex.EncodeToString(sum[:16])
}

type ParsedMeta struct {
	Title              string
	Artist             string
	AlbumArtist        string
	Album              string
	TrackNumber        *int
	DiscNumber         *int
	Year               *int
	Genre              *string
	Duration           float64
	SampleRate         *int
	BitDepth           *int
	Bitrate            *int
	ArtworkData        []byte
	HasEmbeddedArtwork bool
}

func ParseAudioFile(ctx context.Context, fullPath, format string) ParsedMeta {
	var m ParsedMeta
	switch format {
	case "flac":
		m = parseFLAC(fullPath)
	case "mp3":
		m = parseMP3(fullPath)
	case "wav":
		m = parseWAV(fullPath)
	case "ogg":
		m = parseOGG(fullPath)
	case "dsf":
		m = parseDSF(fullPath)
	case "ape":
		m = parseAPE(fullPath)
	}

	// Fallback to ffprobe for M4A/ALAC/AIFF/OPUS/WMA or missing duration/title/artist
	if m.Title == "" || m.Artist == "" || m.Duration <= 0 || format == "m4a" || format == "alac" || format == "aiff" || format == "opus" || format == "wma" {
		mergeFFProbe(ctx, fullPath, &m)
	}
	return m
}

// --- FLAC Parser ---

func parseFLAC(path string) ParsedMeta {
	var m ParsedMeta
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()

	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil || string(magic) != "fLaC" {
		return m
	}

	isLast := false
	hdr := make([]byte, 4)
	for !isLast {
		if _, err := io.ReadFull(f, hdr); err != nil {
			break
		}
		isLast = (hdr[0] & 0x80) != 0
		blockType := hdr[0] & 0x7F
		length := int(hdr[1])<<16 | int(hdr[2])<<8 | int(hdr[3])
		if length < 0 || length > 64_000_000 {
			break
		}

		if blockType == 0 || blockType == 4 || blockType == 6 {
			data := make([]byte, length)
			if _, err := io.ReadFull(f, data); err != nil {
				break
			}
			switch blockType {
			case 0: // STREAMINFO
				if len(data) >= 18 {
					sr := int(data[10])<<12 | int(data[11])<<4 | int(data[12])>>4
					bps := ((int(data[12])&0x01)<<4 | int(data[13])>>4) + 1
					totalSamples := uint64(data[13]&0x0F)<<32 |
						uint64(data[14])<<24 |
						uint64(data[15])<<16 |
						uint64(data[16])<<8 |
						uint64(data[17])
					if sr > 0 {
						m.SampleRate = &sr
					}
					if bps > 0 {
						m.BitDepth = &bps
					}
					if sr > 0 && totalSamples > 0 {
						m.Duration = float64(totalSamples) / float64(sr)
					}
				}
			case 4: // VORBIS_COMMENT
				parseVorbisComments(data, &m)
			case 6: // PICTURE
				if picType, picData, ok := parseFLACPictureBlock(data); ok {
					if len(m.ArtworkData) == 0 || picType == 3 {
						m.ArtworkData = picData
						m.HasEmbeddedArtwork = true
					}
				}
			}
		} else {
			if _, err := f.Seek(int64(length), io.SeekCurrent); err != nil {
				break
			}
		}
	}

	if m.Duration > 0 && m.Bitrate == nil {
		if fi, err := f.Stat(); err == nil && fi.Size() > 0 {
			br := int((float64(fi.Size()) * 8.0) / m.Duration / 1000.0)
			if br > 0 {
				m.Bitrate = &br
			}
		}
	}
	return m
}

func parseVorbisComments(data []byte, m *ParsedMeta) {
	if len(data) < 8 {
		return
	}
	off := 0
	vendorLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	if vendorLen < 0 || off+vendorLen+4 > len(data) {
		return
	}
	off += vendorLen
	count := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	for i := 0; i < count && off+4 <= len(data); i++ {
		l := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		if l < 0 || off+l > len(data) {
			break
		}
		kv := string(data[off : off+l])
		off += l
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		k := strings.ToUpper(strings.TrimSpace(kv[:eq]))
		v := strings.TrimSpace(kv[eq+1:])
		if v == "" {
			continue
		}
		switch k {
		case "TITLE":
			if m.Title == "" {
				m.Title = v
			}
		case "ARTIST":
			if m.Artist == "" {
				m.Artist = v
			}
		case "ALBUMARTIST", "ALBUM ARTIST":
			if m.AlbumArtist == "" {
				m.AlbumArtist = v
			}
		case "ALBUM":
			if m.Album == "" {
				m.Album = v
			}
		case "TRACKNUMBER":
			if m.TrackNumber == nil {
				if n, ok := parseSlashInt(v); ok {
					m.TrackNumber = &n
				}
			}
		case "DISCNUMBER":
			if m.DiscNumber == nil {
				if n, ok := parseSlashInt(v); ok {
					m.DiscNumber = &n
				}
			}
		case "DATE", "YEAR", "ORIGINALDATE":
			if m.Year == nil {
				if y, ok := parseYear(v); ok {
					m.Year = &y
				}
			}
		case "GENRE":
			if m.Genre == nil {
				g := v
				m.Genre = &g
			}
		case "METADATA_BLOCK_PICTURE":
			if raw, err := base64.StdEncoding.DecodeString(v); err == nil {
				if picType, picData, ok := parseFLACPictureBlock(raw); ok {
					if len(m.ArtworkData) == 0 || picType == 3 {
						m.ArtworkData = picData
						m.HasEmbeddedArtwork = true
					}
				}
			}
		}
	}
}

func parseFLACPictureBlock(data []byte) (int, []byte, bool) {
	if len(data) < 32 {
		return 0, nil, false
	}
	off := 0
	picType := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	mimeLen := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4 + mimeLen
	if off+4 > len(data) {
		return 0, nil, false
	}
	descLen := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4 + descLen + 16 // width, height, depth, colors
	if off+4 > len(data) {
		return 0, nil, false
	}
	picLen := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	if picLen <= 0 || off+picLen > len(data) {
		return 0, nil, false
	}
	pic := make([]byte, picLen)
	copy(pic, data[off:off+picLen])
	return picType, pic, true
}

// --- MP3 Parser (ID3v2.2 / v2.3 / v2.4 + ID3v1 + MPEG frame header) ---

func parseMP3(path string) ParsedMeta {
	var m ParsedMeta
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return m
	}
	fileSize := fi.Size()

	hdr := make([]byte, 10)
	var audioStart int64
	if _, err := io.ReadFull(f, hdr); err == nil && string(hdr[:3]) == "ID3" {
		version := hdr[3]
		tagSize := int64(hdr[6]&0x7F)<<21 | int64(hdr[7]&0x7F)<<14 | int64(hdr[8]&0x7F)<<7 | int64(hdr[9]&0x7F)
		audioStart = 10 + tagSize
		if tagSize > 0 && tagSize < 48_000_000 {
			tagData := make([]byte, tagSize)
			if _, err := io.ReadFull(f, tagData); err == nil {
				parseID3v2(tagData, version, &m)
			}
		}
	}

	// Scan for first MPEG audio frame header to estimate sampleRate, bitrate & duration
	if _, err := f.Seek(audioStart, io.SeekStart); err == nil {
		probe := make([]byte, 16384)
		n, _ := f.Read(probe)
		parseMPEGFrame(probe[:n], fileSize-audioStart, &m)
	}

	// ID3v1 fallback if title/artist/album missing
	if (m.Title == "" || m.Artist == "" || m.Album == "") && fileSize >= 128 {
		if _, err := f.Seek(fileSize-128, io.SeekStart); err == nil {
			v1 := make([]byte, 128)
			if _, err := io.ReadFull(f, v1); err == nil && string(v1[:3]) == "TAG" {
				if m.Title == "" {
					m.Title = cleanID3v1(v1[3:33])
				}
				if m.Artist == "" {
					m.Artist = cleanID3v1(v1[33:63])
				}
				if m.Album == "" {
					m.Album = cleanID3v1(v1[63:93])
				}
				if m.Year == nil {
					if y, ok := parseYear(cleanID3v1(v1[93:97])); ok {
						m.Year = &y
					}
				}
				if m.TrackNumber == nil && v1[125] == 0 && v1[126] > 0 {
					tn := int(v1[126])
					m.TrackNumber = &tn
				}
			}
		}
	}
	return m
}

func parseID3v2(data []byte, version byte, m *ParsedMeta) {
	off := 0
	for off < len(data) {
		var frameID string
		var frameSize int
		if version == 2 {
			if off+6 > len(data) {
				break
			}
			if data[off] == 0 {
				break
			}
			frameID = string(data[off : off+3])
			frameSize = int(data[off+3])<<16 | int(data[off+4])<<8 | int(data[off+5])
			off += 6
		} else {
			if off+10 > len(data) {
				break
			}
			if data[off] == 0 {
				break
			}
			frameID = string(data[off : off+4])
			if version == 4 {
				frameSize = int(data[off+4]&0x7F)<<21 | int(data[off+5]&0x7F)<<14 | int(data[off+6]&0x7F)<<7 | int(data[off+7]&0x7F)
			} else {
				frameSize = int(binary.BigEndian.Uint32(data[off+4 : off+8]))
			}
			off += 10
		}
		if frameSize <= 0 || off+frameSize > len(data) {
			break
		}
		payload := data[off : off+frameSize]
		off += frameSize

		switch frameID {
		case "TIT2", "TT2":
			if v := decodeID3Text(payload); v != "" {
				m.Title = v
			}
		case "TPE1", "TP1":
			if v := decodeID3Text(payload); v != "" {
				m.Artist = v
			}
		case "TPE2", "TP2":
			if v := decodeID3Text(payload); v != "" {
				m.AlbumArtist = v
			}
		case "TALB", "TAL":
			if v := decodeID3Text(payload); v != "" {
				m.Album = v
			}
		case "TRCK", "TRK":
			if n, ok := parseSlashInt(decodeID3Text(payload)); ok {
				m.TrackNumber = &n
			}
		case "TPOS", "TPA":
			if n, ok := parseSlashInt(decodeID3Text(payload)); ok {
				m.DiscNumber = &n
			}
		case "TDRC", "TYER", "TYE":
			if y, ok := parseYear(decodeID3Text(payload)); ok {
				m.Year = &y
			}
		case "TCON", "TCO":
			if v := cleanID3Genre(decodeID3Text(payload)); v != "" {
				m.Genre = &v
			}
		case "APIC":
			if picType, img, ok := parseAPIC(payload); ok {
				if len(m.ArtworkData) == 0 || picType == 3 {
					m.ArtworkData = img
					m.HasEmbeddedArtwork = true
				}
			}
		case "PIC":
			if len(payload) > 6 {
				enc := payload[0]
				picType := int(payload[4])
				rest := payload[5:]
				img := stripNullTerminated(rest, enc == 1 || enc == 2)
				if len(img) > 0 && (len(m.ArtworkData) == 0 || picType == 3) {
					m.ArtworkData = append([]byte(nil), img...)
					m.HasEmbeddedArtwork = true
				}
			}
		}
	}
}

func decodeID3Text(payload []byte) string {
	if len(payload) < 2 {
		return ""
	}
	enc := payload[0]
	raw := payload[1:]
	switch enc {
	case 0: // ISO-8859-1
		raw = bytes.TrimRight(raw, "\x00")
		runes := make([]rune, len(raw))
		for i, b := range raw {
			runes[i] = rune(b)
		}
		return strings.TrimSpace(string(runes))
	case 1, 2: // UTF-16 with BOM (1) or UTF-16BE (2)
		return strings.TrimSpace(decodeUTF16(raw, enc == 2))
	case 3: // UTF-8
		return strings.TrimSpace(string(bytes.TrimRight(raw, "\x00")))
	default:
		return strings.TrimSpace(string(bytes.TrimRight(raw, "\x00")))
	}
}

func decodeUTF16(b []byte, forceBE bool) string {
	if len(b) < 2 {
		return ""
	}
	bigEndian := forceBE
	if !forceBE && len(b) >= 2 {
		if b[0] == 0xFE && b[1] == 0xFF {
			bigEndian = true
			b = b[2:]
		} else if b[0] == 0xFF && b[1] == 0xFE {
			bigEndian = false
			b = b[2:]
		}
	}
	u16 := make([]uint16, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		var v uint16
		if bigEndian {
			v = uint16(b[i])<<8 | uint16(b[i+1])
		} else {
			v = uint16(b[i+1])<<8 | uint16(b[i])
		}
		if v == 0 {
			break
		}
		u16 = append(u16, v)
	}
	return string(utf16.Decode(u16))
}

func parseAPIC(payload []byte) (int, []byte, bool) {
	if len(payload) < 6 {
		return 0, nil, false
	}
	enc := payload[0]
	idx := bytes.IndexByte(payload[1:], 0)
	if idx < 0 {
		return 0, nil, false
	}
	picTypePos := 1 + idx + 1
	if picTypePos >= len(payload) {
		return 0, nil, false
	}
	picType := int(payload[picTypePos])
	rest := payload[picTypePos+1:]
	img := stripNullTerminated(rest, enc == 1 || enc == 2)
	if len(img) == 0 {
		return 0, nil, false
	}
	out := make([]byte, len(img))
	copy(out, img)
	return picType, out, true
}

func stripNullTerminated(b []byte, utf16Mode bool) []byte {
	if utf16Mode {
		for i := 0; i+1 < len(b); i += 2 {
			if b[i] == 0 && b[i+1] == 0 {
				return b[i+2:]
			}
		}
		return nil
	}
	if idx := bytes.IndexByte(b, 0); idx >= 0 {
		return b[idx+1:]
	}
	return nil
}

var mpeg1L3Bitrates = []int{0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0}
var mpeg2L3Bitrates = []int{0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0}
var mpeg1SR = []int{44100, 48000, 32000, 0}
var mpeg2SR = []int{22050, 24000, 16000, 0}

func parseMPEGFrame(buf []byte, audioBytes int64, m *ParsedMeta) {
	for i := 0; i+4 <= len(buf); i++ {
		if buf[i] != 0xFF || (buf[i+1]&0xE0) != 0xE0 {
			continue
		}
		ver := (buf[i+1] >> 3) & 0x03
		layer := (buf[i+1] >> 1) & 0x03
		brIdx := (buf[i+2] >> 4) & 0x0F
		srIdx := (buf[i+2] >> 2) & 0x03
		if ver == 1 || layer != 1 || brIdx == 0 || brIdx == 15 || srIdx == 3 {
			continue
		}
		var br, sr, spf int
		if ver == 3 { // MPEG1
			br = mpeg1L3Bitrates[brIdx]
			sr = mpeg1SR[srIdx]
			spf = 1152
		} else {
			br = mpeg2L3Bitrates[brIdx]
			sr = mpeg2SR[srIdx]
			spf = 576
		}
		if sr > 0 {
			m.SampleRate = &sr
		}
		if br > 0 {
			m.Bitrate = &br
		}

		// Check Xing/Info VBR header inside frame
		channelMode := (buf[i+3] >> 6) & 0x03
		xingOff := 36
		if ver == 3 && channelMode == 3 {
			xingOff = 21
		} else if ver != 3 {
			if channelMode == 3 {
				xingOff = 13
			} else {
				xingOff = 21
			}
		}
		if i+xingOff+12 <= len(buf) {
			sig := string(buf[i+xingOff : i+xingOff+4])
			if sig == "Xing" || sig == "Info" {
				flags := binary.BigEndian.Uint32(buf[i+xingOff+4 : i+xingOff+8])
				if flags&1 != 0 && sr > 0 {
					frames := binary.BigEndian.Uint32(buf[i+xingOff+8 : i+xingOff+12])
					if frames > 0 {
						m.Duration = float64(frames) * float64(spf) / float64(sr)
						if audioBytes > 0 && m.Duration > 0 {
							avgBR := int(float64(audioBytes) * 8.0 / m.Duration / 1000.0)
							if avgBR > 0 {
								m.Bitrate = &avgBR
							}
						}
						return
					}
				}
			}
		}

		// CBR duration estimate
		if br > 0 && audioBytes > 0 {
			m.Duration = float64(audioBytes) * 8.0 / float64(br*1000)
		}
		return
	}
}

func cleanID3v1(b []byte) string {
	return strings.TrimSpace(string(bytes.TrimRight(b, "\x00 ")))
}

var genreParenRe = regexp.MustCompile(`^\(\d+\)\s*`)

func cleanID3Genre(s string) string {
	s = strings.TrimSpace(genreParenRe.ReplaceAllString(s, ""))
	return s
}

// --- WAV Parser ---

func parseWAV(path string) ParsedMeta {
	var m ParsedMeta
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()

	hdr := make([]byte, 12)
	if _, err := io.ReadFull(f, hdr); err != nil || string(hdr[:4]) != "RIFF" || string(hdr[8:12]) != "WAVE" {
		return m
	}

	var byteRate uint32
	var dataSize uint32
	chunkHdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(f, chunkHdr); err != nil {
			break
		}
		id := string(chunkHdr[:4])
		size := binary.LittleEndian.Uint32(chunkHdr[4:8])
		padded := int64(size)
		if padded%2 == 1 {
			padded++
		}
		switch id {
		case "fmt ":
			if size >= 16 && size < 4096 {
				b := make([]byte, size)
				if _, err := io.ReadFull(f, b); err == nil {
					sr := int(binary.LittleEndian.Uint32(b[4:8]))
					byteRate = binary.LittleEndian.Uint32(b[8:12])
					bps := int(binary.LittleEndian.Uint16(b[14:16]))
					if sr > 0 {
						m.SampleRate = &sr
					}
					if bps > 0 {
						m.BitDepth = &bps
					}
					if byteRate > 0 {
						br := int(byteRate * 8 / 1000)
						m.Bitrate = &br
					}
				}
				if padded > int64(size) {
					_, _ = f.Seek(padded-int64(size), io.SeekCurrent)
				}
			} else {
				_, _ = f.Seek(padded, io.SeekCurrent)
			}
		case "data":
			dataSize = size
			_, _ = f.Seek(padded, io.SeekCurrent)
		case "LIST":
			if size >= 4 && size < 1_000_000 {
				b := make([]byte, size)
				if _, err := io.ReadFull(f, b); err == nil && string(b[:4]) == "INFO" {
					parseWAVInfo(b[4:], &m)
				}
				if padded > int64(size) {
					_, _ = f.Seek(padded-int64(size), io.SeekCurrent)
				}
			} else {
				_, _ = f.Seek(padded, io.SeekCurrent)
			}
		case "id3 ", "ID3 ":
			if size > 10 && size < 32_000_000 {
				b := make([]byte, size)
				if _, err := io.ReadFull(f, b); err == nil && string(b[:3]) == "ID3" {
					parseID3v2(b[10:], b[3], &m)
				}
				if padded > int64(size) {
					_, _ = f.Seek(padded-int64(size), io.SeekCurrent)
				}
			} else {
				_, _ = f.Seek(padded, io.SeekCurrent)
			}
		default:
			if _, err := f.Seek(padded, io.SeekCurrent); err != nil {
				break
			}
		}
	}
	if byteRate > 0 && dataSize > 0 {
		m.Duration = float64(dataSize) / float64(byteRate)
	}
	return m
}

func parseWAVInfo(b []byte, m *ParsedMeta) {
	off := 0
	for off+8 <= len(b) {
		id := string(b[off : off+4])
		sz := int(binary.LittleEndian.Uint32(b[off+4 : off+8]))
		off += 8
		if sz < 0 || off+sz > len(b) {
			break
		}
		val := strings.TrimSpace(string(bytes.TrimRight(b[off:off+sz], "\x00")))
		off += sz
		if sz%2 == 1 {
			off++
		}
		switch id {
		case "INAM":
			if m.Title == "" {
				m.Title = val
			}
		case "IART":
			if m.Artist == "" {
				m.Artist = val
			}
		case "IPRD":
			if m.Album == "" {
				m.Album = val
			}
		case "ICRD":
			if m.Year == nil {
				if y, ok := parseYear(val); ok {
					m.Year = &y
				}
			}
		case "IGNR":
			if m.Genre == nil && val != "" {
				g := val
				m.Genre = &g
			}
		case "ITRK":
			if m.TrackNumber == nil {
				if n, ok := parseSlashInt(val); ok {
					m.TrackNumber = &n
				}
			}
		}
	}
}

// --- OGG Vorbis Parser ---

func parseOGG(path string) ParsedMeta {
	var m ParsedMeta
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()

	buf := make([]byte, 262_144)
	n, _ := f.Read(buf)
	head := buf[:n]

	// Locate Vorbis identification header ("\x01vorbis")
	if idx := bytes.Index(head, []byte("\x01vorbis")); idx >= 0 && idx+28 <= len(head) {
		sr := int(binary.LittleEndian.Uint32(head[idx+12 : idx+16]))
		if sr > 0 {
			m.SampleRate = &sr
		}
		brNominal := int(binary.LittleEndian.Uint32(head[idx+20 : idx+24]))
		if brNominal > 0 {
			br := brNominal / 1000
			m.Bitrate = &br
		}
	}
	// Locate Vorbis comment header ("\x03vorbis")
	if idx := bytes.Index(head, []byte("\x03vorbis")); idx >= 0 {
		parseVorbisComments(head[idx+7:], &m)
	}
	// Estimate duration from last OggS page granule position
	if fi, err := f.Stat(); err == nil && m.SampleRate != nil && *m.SampleRate > 0 {
		tailSize := min(int64(65536), fi.Size())
		if _, err := f.Seek(fi.Size()-tailSize, io.SeekStart); err == nil {
			tail := make([]byte, tailSize)
			tn, _ := f.Read(tail)
			if pos := bytes.LastIndex(tail[:tn], []byte("OggS")); pos >= 0 && pos+14 <= tn {
				granule := binary.LittleEndian.Uint64(tail[pos+6 : pos+14])
				if granule > 0 && granule != ^uint64(0) {
					m.Duration = float64(granule) / float64(*m.SampleRate)
				}
			}
		}
	}
	return m
}

// --- DSF Parser ---

func parseDSF(path string) ParsedMeta {
	var m ParsedMeta
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()

	hdr := make([]byte, 80)
	if _, err := io.ReadFull(f, hdr); err != nil || string(hdr[:4]) != "DSD " {
		return m
	}
	metaOff := int64(binary.LittleEndian.Uint64(hdr[20:28]))
	if string(hdr[28:32]) == "fmt " {
		sr := int(binary.LittleEndian.Uint32(hdr[56:60]))
		bps := int(binary.LittleEndian.Uint32(hdr[60:64]))
		samples := binary.LittleEndian.Uint64(hdr[64:72])
		if sr > 0 {
			m.SampleRate = &sr
		}
		if bps > 0 {
			m.BitDepth = &bps
		}
		if sr > 0 && samples > 0 {
			m.Duration = float64(samples) / float64(sr)
		}
	}
	if metaOff > 0 {
		if _, err := f.Seek(metaOff, io.SeekStart); err == nil {
			id3Hdr := make([]byte, 10)
			if _, err := io.ReadFull(f, id3Hdr); err == nil && string(id3Hdr[:3]) == "ID3" {
				sz := int(id3Hdr[6]&0x7F)<<21 | int(id3Hdr[7]&0x7F)<<14 | int(id3Hdr[8]&0x7F)<<7 | int(id3Hdr[9]&0x7F)
				if sz > 0 && sz < 32_000_000 {
					tag := make([]byte, sz)
					if _, err := io.ReadFull(f, tag); err == nil {
						parseID3v2(tag, id3Hdr[3], &m)
					}
				}
			}
		}
	}
	return m
}

// --- APEv2 Footer Parser ---

func parseAPE(path string) ParsedMeta {
	var m ParsedMeta
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil || fi.Size() < 32 {
		return m
	}
	footer := make([]byte, 32)
	if _, err := f.Seek(fi.Size()-32, io.SeekStart); err != nil {
		return m
	}
	if _, err := io.ReadFull(f, footer); err != nil || string(footer[:8]) != "APETAGEX" {
		return m
	}
	tagSize := int64(binary.LittleEndian.Uint32(footer[12:16]))
	itemCount := int(binary.LittleEndian.Uint32(footer[16:20]))
	if tagSize <= 32 || tagSize > 32_000_000 || fi.Size() < tagSize {
		return m
	}
	body := make([]byte, tagSize-32)
	if _, err := f.Seek(fi.Size()-tagSize, io.SeekStart); err != nil {
		return m
	}
	if _, err := io.ReadFull(f, body); err != nil {
		return m
	}
	off := 0
	for i := 0; i < itemCount && off+8 < len(body); i++ {
		valLen := int(binary.LittleEndian.Uint32(body[off : off+4]))
		flags := binary.LittleEndian.Uint32(body[off+4 : off+8])
		off += 8
		nul := bytes.IndexByte(body[off:], 0)
		if nul < 0 {
			break
		}
		key := strings.ToUpper(string(body[off : off+nul]))
		off += nul + 1
		if valLen < 0 || off+valLen > len(body) {
			break
		}
		valBytes := body[off : off+valLen]
		off += valLen
		isBinary := ((flags >> 1) & 0x03) == 1
		if isBinary && strings.Contains(key, "COVER") {
			if idx := bytes.IndexByte(valBytes, 0); idx >= 0 && idx+1 < len(valBytes) {
				m.ArtworkData = append([]byte(nil), valBytes[idx+1:]...)
				m.HasEmbeddedArtwork = true
			}
			continue
		}
		val := strings.TrimSpace(string(valBytes))
		switch key {
		case "TITLE":
			m.Title = val
		case "ARTIST":
			m.Artist = val
		case "ALBUM ARTIST", "ALBUMARTIST":
			m.AlbumArtist = val
		case "ALBUM":
			m.Album = val
		case "TRACK":
			if n, ok := parseSlashInt(val); ok {
				m.TrackNumber = &n
			}
		case "DISC":
			if n, ok := parseSlashInt(val); ok {
				m.DiscNumber = &n
			}
		case "YEAR", "DATE":
			if y, ok := parseYear(val); ok {
				m.Year = &y
			}
		case "GENRE":
			if val != "" {
				g := val
				m.Genre = &g
			}
		}
	}
	return m
}

// --- ffprobe Fallback ---

type ffprobeOutput struct {
	Streams []struct {
		CodecType     string `json:"codec_type"`
		CodecName     string `json:"codec_name"`
		SampleRate    string `json:"sample_rate"`
		BitsPerSample int    `json:"bits_per_sample"`
		BitsPerRaw    string `json:"bits_per_raw_sample"`
		BitRate       string `json:"bit_rate"`
	} `json:"streams"`
	Format struct {
		Duration string            `json:"duration"`
		BitRate  string            `json:"bit_rate"`
		Tags     map[string]string `json:"tags"`
	} `json:"format"`
}

func mergeFFProbe(ctx context.Context, path string, m *ParsedMeta) {
	probeBin, err := exec.LookPath("ffprobe")
	if err != nil {
		return
	}
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(tctx, probeBin,
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		path,
	)
	out, err := cmd.Output()
	if err != nil {
		return
	}
	var parsed ffprobeOutput
	if err := json.Unmarshal(out, &parsed); err != nil {
		return
	}
	if m.Duration <= 0 && parsed.Format.Duration != "" {
		if d, err := strconv.ParseFloat(parsed.Format.Duration, 64); err == nil && d > 0 {
			m.Duration = d
		}
	}
	if m.Bitrate == nil && parsed.Format.BitRate != "" {
		if br, err := strconv.Atoi(parsed.Format.BitRate); err == nil && br > 0 {
			kbps := br / 1000
			m.Bitrate = &kbps
		}
	}
	hasAttachedPic := false
	for _, st := range parsed.Streams {
		if st.CodecType == "audio" {
			if m.SampleRate == nil && st.SampleRate != "" {
				if sr, err := strconv.Atoi(st.SampleRate); err == nil && sr > 0 {
					m.SampleRate = &sr
				}
			}
			if m.BitDepth == nil {
				if st.BitsPerSample > 0 {
					bp := st.BitsPerSample
					m.BitDepth = &bp
				} else if st.BitsPerRaw != "" {
					if bp, err := strconv.Atoi(st.BitsPerRaw); err == nil && bp > 0 {
						m.BitDepth = &bp
					}
				}
			}
		} else if st.CodecType == "video" {
			hasAttachedPic = true
		}
	}

	tags := make(map[string]string, len(parsed.Format.Tags))
	for k, v := range parsed.Format.Tags {
		tags[strings.ToLower(k)] = strings.TrimSpace(v)
	}
	if m.Title == "" {
		m.Title = tags["title"]
	}
	if m.Artist == "" {
		m.Artist = tags["artist"]
	}
	if m.AlbumArtist == "" {
		m.AlbumArtist = firstNonEmpty(tags["album_artist"], tags["albumartist"])
	}
	if m.Album == "" {
		m.Album = tags["album"]
	}
	if m.TrackNumber == nil {
		if n, ok := parseSlashInt(tags["track"]); ok {
			m.TrackNumber = &n
		}
	}
	if m.DiscNumber == nil {
		if n, ok := parseSlashInt(tags["disc"]); ok {
			m.DiscNumber = &n
		}
	}
	if m.Year == nil {
		if y, ok := parseYear(firstNonEmpty(tags["date"], tags["year"])); ok {
			m.Year = &y
		}
	}
	if m.Genre == nil && tags["genre"] != "" {
		g := tags["genre"]
		m.Genre = &g
	}

	// Extract embedded cover art via ffmpeg if present (e.g. M4A/ALAC)
	if len(m.ArtworkData) == 0 && hasAttachedPic {
		if ffBin, err := exec.LookPath("ffmpeg"); err == nil {
			ectx, ecancel := context.WithTimeout(ctx, 8*time.Second)
			defer ecancel()
			ecmd := exec.CommandContext(ectx, ffBin, "-v", "quiet", "-i", path, "-an", "-vcodec", "copy", "-f", "image2pipe", "-")
			if img, err := ecmd.Output(); err == nil && len(img) > 0 {
				m.ArtworkData = img
				m.HasEmbeddedArtwork = true
			}
		}
	}
}

// --- Folder Artwork & Path Fallback Helpers ---

var folderArtNames = []string{
	"cover.jpg", "cover.jpeg", "cover.png",
	"folder.jpg", "folder.jpeg", "folder.png",
	"front.jpg", "front.jpeg", "front.png",
	"album.jpg", "album.jpeg", "album.png",
	"Cover.jpg", "Folder.jpg", "Front.jpg",
}

func FindFolderArtwork(dir string) []byte {
	for _, name := range folderArtNames {
		p := filepath.Join(dir, name)
		if data, err := os.ReadFile(p); err == nil && len(data) > 0 {
			return data
		}
	}
	return nil
}

var leadingTrackRe = regexp.MustCompile(`^(\d{1,3})\s*[-._)]\s*(.+)$`)

func InferFromPath(relPath string) (title, artist, album string, trackNum *int) {
	base := strings.TrimSuffix(filepath.Base(relPath), filepath.Ext(relPath))
	title = base
	if matches := leadingTrackRe.FindStringSubmatch(base); len(matches) == 3 {
		if n, err := strconv.Atoi(matches[1]); err == nil && n > 0 {
			trackNum = &n
		}
		title = strings.TrimSpace(matches[2])
	}
	dir := filepath.Dir(relPath)
	if dir != "." && dir != "" {
		album = filepath.Base(dir)
		parent := filepath.Dir(dir)
		if parent != "." && parent != "" {
			artist = filepath.Base(parent)
		}
	}
	if artist == "" {
		artist = "Unknown Artist"
	}
	if album == "" {
		album = "Unknown Album"
	}
	return
}

func parseSlashInt(s string) (int, bool) {
	s = strings.TrimSpace(s)
	if idx := strings.IndexByte(s, '/'); idx >= 0 {
		s = s[:idx]
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

var yearRe = regexp.MustCompile(`\b(19\d{2}|20\d{2})\b`)

func parseYear(s string) (int, bool) {
	m := yearRe.FindString(s)
	if m == "" {
		return 0, false
	}
	y, err := strconv.Atoi(m)
	return y, err == nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
