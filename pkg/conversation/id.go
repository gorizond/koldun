package conversation

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	maxIdentifierLength = 63
	defaultHashLen      = 24
)

var allowedChars = regexp.MustCompile(`[^a-z0-9]`)

// IDOptions customises MakeID behaviour.
type IDOptions struct {
	Secret      []byte
	HashLength  int
	HumanPrefix bool
	Now         func() time.Time
}

// MakeID constructs a stable identifier derived from the requesting token, chat identifier,
// and chat start timestamp. The implementation mirrors the Python reference provided in the
// product specification.
func MakeID(userKey, chatID, chatStart string, opts *IDOptions) (string, error) {
	if strings.TrimSpace(userKey) == "" {
		return "", errors.New("user key must not be empty")
	}
	if strings.TrimSpace(chatID) == "" {
		return "", errors.New("chat id must not be empty")
	}
	if strings.TrimSpace(chatStart) == "" {
		return "", errors.New("chat start time must not be empty")
	}

	cfg := IDOptions{
		HashLength:  defaultHashLen,
		HumanPrefix: true,
		Now:         time.Now,
	}
	if opts != nil {
		if opts.HashLength > 0 {
			cfg.HashLength = opts.HashLength
		}
		if opts.Secret != nil {
			cfg.Secret = opts.Secret
		}
		if !opts.HumanPrefix {
			cfg.HumanPrefix = false
		}
		if opts.Now != nil {
			cfg.Now = opts.Now
		}
	}

	payload := struct {
		ChatStart string `json:"chat_start_time"`
		Session   string `json:"device_session"`
		UserKey   string `json:"user_key"`
		Version   int    `json:"v"`
	}{
		ChatStart: chatStart,
		Session:   chatID,
		UserKey:   userKey,
		Version:   1,
	}

	canon, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	var digest []byte
	if len(cfg.Secret) > 0 {
		h := hmac.New(sha256.New, cfg.Secret)
		_, _ = h.Write(canon)
		digest = h.Sum(nil)
	} else {
		sum := sha256.Sum256(canon)
		digest = sum[:]
	}

	n := digest[:12]
	base := base36FromBytes(n)
	if len(base) > cfg.HashLength {
		base = base[:cfg.HashLength]
	}

	ts := parseTimestamp(chatStart)
	if ts == 0 {
		ts = cfg.Now().Unix()
	}
	timePart := base36FromInt(ts)

	parts := make([]string, 0, 3)
	if cfg.HumanPrefix {
		prefix := normalise(userKey)
		if len(prefix) > 12 {
			prefix = prefix[:12]
		}
		parts = append(parts, prefix)
	}
	parts = append(parts, timePart)
	parts = append(parts, base)

	identifier := strings.Join(parts, "")
	if len(identifier) > maxIdentifierLength {
		identifier = identifier[:maxIdentifierLength]
	}
	if identifier == "" {
		identifier = "x"
	}
	return identifier, nil
}

func base36FromBytes(b []byte) string {
	if len(b) == 0 {
		return "0"
	}
	bi := new(big.Int).SetBytes(b)
	return base36FromBig(bi)
}

func base36FromInt(v int64) string {
	if v <= 0 {
		return "0"
	}
	return base36FromBig(big.NewInt(v))
}

func base36FromBig(v *big.Int) string {
	if v.Sign() <= 0 {
		return "0"
	}
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	base := big.NewInt(36)
	zero := big.NewInt(0)
	result := make([]byte, 0, 16)
	tmp := new(big.Int).Set(v)
	rem := new(big.Int)
	for tmp.Cmp(zero) > 0 {
		tmp.QuoRem(tmp, base, rem)
		result = append([]byte{alphabet[rem.Int64()]}, result...)
	}
	return string(result)
}

func parseTimestamp(raw string) int64 {
	if raw == "" {
		return 0
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return v
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.Unix()
	}
	return 0
}

func normalise(input string) string {
	s := strings.ToLower(input)
	s = allowedChars.ReplaceAllString(s, "")
	if s == "" {
		return "x"
	}
	return s
}
