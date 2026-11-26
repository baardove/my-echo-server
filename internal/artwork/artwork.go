package artwork

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"image"
	"image/color"
	"image/draw"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"os"
	"path/filepath"
	"sync"
)

const (
	MaxThumbDimension = 512
	MaxThumbBytes     = 220_000
)

// Cache stores original artwork bytes and strict sRGB Baseline JPEG 512px thumbnails,
// keyed by SHA256(raw)[:16] hex — identical to EchoCore ArtworkCache.hash(for:).
type Cache struct {
	dir string
	mu  sync.RWMutex
}

func NewCache(dir string) (*Cache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Cache{dir: dir}, nil
}

// Hash computes the exact same 32-char lowercase hex prefix (first 16 bytes of SHA-256) as Swift ArtworkCache.
func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:16])
}

func IsSafeHash(h string) bool {
	if len(h) == 0 || len(h) > 128 {
		return false
	}
	for _, r := range h {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func (c *Cache) ThumbPath(hash string) string {
	return filepath.Join(c.dir, hash+"_thumb.jpg")
}

func (c *Cache) OrigPath(hash string) string {
	return filepath.Join(c.dir, hash+"_orig.dat")
}

func (c *Cache) Store(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	key := Hash(raw)
	thumbPath := c.ThumbPath(key)
	origPath := c.OrigPath(key)

	c.mu.RLock()
	_, err := os.Stat(thumbPath)
	c.mu.RUnlock()
	if err == nil {
		return key
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := os.Stat(thumbPath); err == nil {
		return key
	}

	_ = atomicWrite(origPath, raw)

	thumb := convertToBaselineJPEG(raw, MaxThumbDimension, MaxThumbBytes)
	if len(thumb) > 0 {
		_ = atomicWrite(thumbPath, thumb)
	} else {
		_ = atomicWrite(thumbPath, raw)
	}
	return key
}

func (c *Cache) LoadThumb(hash string) ([]byte, error) {
	if !IsSafeHash(hash) {
		return nil, os.ErrNotExist
	}
	if data, err := os.ReadFile(c.ThumbPath(hash)); err == nil {
		return data, nil
	}
	return os.ReadFile(c.OrigPath(hash))
}

func (c *Cache) LoadOrig(hash string) ([]byte, error) {
	if !IsSafeHash(hash) {
		return nil, os.ErrNotExist
	}
	if data, err := os.ReadFile(c.OrigPath(hash)); err == nil {
		return data, nil
	}
	return os.ReadFile(c.ThumbPath(hash))
}

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func convertToBaselineJPEG(raw []byte, maxDim, maxBytes int) []byte {
	src, _, err := image.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil
	}
	dst := resizeBilinearSRGB(src, maxDim)
	qualities := []int{85, 75, 65, 55, 45}
	var last []byte
	for _, q := range qualities {
		var buf bytes.Buffer
		if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: q}); err == nil {
			last = buf.Bytes()
			if len(last) <= maxBytes {
				return last
			}
		}
	}
	return last
}

// resizeBilinearSRGB converts any image (including RGBA with alpha or CMYK) to opaque sRGB resized to <= maxDim.
func resizeBilinearSRGB(src image.Image, maxDim int) *image.RGBA {
	b := src.Bounds()
	srcW, srcH := b.Dx(), b.Dy()
	if srcW <= 0 || srcH <= 0 {
		return image.NewRGBA(image.Rect(0, 0, 1, 1))
	}

	dstW, dstH := srcW, srcH
	if srcW > maxDim || srcH > maxDim {
		if srcW >= srcH {
			dstW = maxDim
			dstH = max(1, (srcH*maxDim)/srcW)
		} else {
			dstH = maxDim
			dstW = max(1, (srcW*maxDim)/srcH)
		}
	}

	if dstW == srcW && dstH == srcH {
		out := image.NewRGBA(image.Rect(0, 0, dstW, dstH))
		draw.Draw(out, out.Bounds(), &image.Uniform{color.Black}, image.Point{}, draw.Src)
		draw.Draw(out, out.Bounds(), src, b.Min, draw.Over)
		return out
	}

	// Pre-flatten source to opaque RGBA for fast pixel reads
	flat := image.NewRGBA(image.Rect(0, 0, srcW, srcH))
	draw.Draw(flat, flat.Bounds(), &image.Uniform{color.Black}, image.Point{}, draw.Src)
	draw.Draw(flat, flat.Bounds(), src, b.Min, draw.Over)

	out := image.NewRGBA(image.Rect(0, 0, dstW, dstH))
	scaleX := float64(srcW) / float64(dstW)
	scaleY := float64(srcH) / float64(dstH)

	for y := 0; y < dstH; y++ {
		sy := (float64(y)+0.5)*scaleY - 0.5
		if sy < 0 {
			sy = 0
		}
		y0 := int(sy)
		y1 := min(y0+1, srcH-1)
		fy := sy - float64(y0)
		for x := 0; x < dstW; x++ {
			sx := (float64(x)+0.5)*scaleX - 0.5
			if sx < 0 {
				sx = 0
			}
			x0 := int(sx)
			x1 := min(x0+1, srcW-1)
			fx := sx - float64(x0)

			o00 := flat.PixOffset(x0, y0)
			o10 := flat.PixOffset(x1, y0)
			o01 := flat.PixOffset(x0, y1)
			o11 := flat.PixOffset(x1, y1)

			w00 := (1 - fx) * (1 - fy)
			w10 := fx * (1 - fy)
			w01 := (1 - fx) * fy
			w11 := fx * fy

			r := float64(flat.Pix[o00])*w00 + float64(flat.Pix[o10])*w10 + float64(flat.Pix[o01])*w01 + float64(flat.Pix[o11])*w11
			g := float64(flat.Pix[o00+1])*w00 + float64(flat.Pix[o10+1])*w10 + float64(flat.Pix[o01+1])*w01 + float64(flat.Pix[o11+1])*w11
			bl := float64(flat.Pix[o00+2])*w00 + float64(flat.Pix[o10+2])*w10 + float64(flat.Pix[o01+2])*w01 + float64(flat.Pix[o11+2])*w11

			do := out.PixOffset(x, y)
			out.Pix[do] = uint8(r + 0.5)
			out.Pix[do+1] = uint8(g + 0.5)
			out.Pix[do+2] = uint8(bl + 0.5)
			out.Pix[do+3] = 255
		}
	}
	return out
}
