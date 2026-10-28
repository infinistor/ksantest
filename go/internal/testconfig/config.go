package testconfig

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type User struct {
	DisplayName, ID, AccountID, Email, AccessKey, SecretKey, KMS, XAuthToken string
}

// ExpectedOwnerID는 x-amz-expected-bucket-owner 등 버킷 소유자 검증에 쓰는 아이디다.
// AWS는 12자리 계정 아이디를 요구하므로 설정에 AccountId가 있으면 그 값을,
// 없으면(ksan 등) 소유자 아이디를 그대로 사용한다.
func (u User) ExpectedOwnerID() string {
	if strings.TrimSpace(u.AccountID) != "" {
		return u.AccountID
	}
	return u.ID
}

type Config struct {
	URL, Region, BucketPrefix string
	Port, OldPort, SSLPort    int
	SignatureVersion          int
	Secure, NotDelete         bool
	Main, Alt, Backend        User

	// WebsiteURL은 웹사이트 엔드포인트 템플릿이다. {bucket}이 버킷명으로 치환된다. 비어 있으면 접속 테스트를 건너뛴다.
	WebsiteURL string
}

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	values := map[string]map[string]string{}
	section := "Default"
	values[section] = map[string]string{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(s.Text(), "\ufeff"))
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			if values[section] == nil {
				values[section] = map[string]string{}
			}
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return Config{}, fmt.Errorf("invalid INI line: %q", line)
		}
		values[section][strings.TrimSpace(key)] = strings.TrimSpace(val)
	}
	if err := s.Err(); err != nil {
		return Config{}, err
	}
	get := func(sec, key string) string { return values[sec][key] }
	integer := func(sec, key string) int { n, _ := strconv.Atoi(get(sec, key)); return n }
	boolean := func(sec, key string) bool { b, _ := strconv.ParseBool(get(sec, key)); return b }
	user := func(sec string) User {
		return User{get(sec, "DisplayName"), get(sec, "UserID"), get(sec, "AccountId"), get(sec, "Email"), get(sec, "AccessKey"), get(sec, "SecretKey"), get(sec, "KMS"), get(sec, "XAuthToken")}
	}
	c := Config{
		URL: get("S3", "URL"), Port: integer("S3", "Port"), OldPort: integer("S3", "OldPort"), SSLPort: integer("S3", "SSLPort"),
		SignatureVersion: integer("S3", "SignatureVersion"), Region: get("S3", "RegionName"), WebsiteURL: get("S3", "WebsiteURL"),
		BucketPrefix: get("Fixtures", "BucketPrefix"), Secure: boolean("Fixtures", "IsSecure"), NotDelete: boolean("Fixtures", "NotDelete"),
		Main: user("Main User"), Alt: user("Alt User"), Backend: user("Backend User"),
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.BucketPrefix == "" {
		c.BucketPrefix = "go-"
	}
	return c, nil
}

// HasWebsiteURL은 웹사이트 엔드포인트가 설정되어 있는지 확인한다.
func (c Config) HasWebsiteURL() bool {
	return strings.TrimSpace(c.WebsiteURL) != ""
}

func (c Config) Endpoint() string {
	if c.URL == "" {
		return ""
	}
	scheme, port := "http", c.Port
	if c.Secure {
		scheme, port = "https", c.SSLPort
	}
	if strings.HasPrefix(c.URL, "http://") || strings.HasPrefix(c.URL, "https://") {
		return strings.TrimRight(c.URL, "/")
	}
	if port <= 0 || (scheme == "http" && port == 80) || (scheme == "https" && port == 443) {
		return scheme + "://" + c.URL
	}
	return fmt.Sprintf("%s://%s:%d", scheme, c.URL, port)
}
