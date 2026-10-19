package pricing

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// awsConfigPath is the aws CLI's config file ($AWS_CONFIG_FILE or
// ~/.aws/config).
func awsConfigPath() string {
	if p := os.Getenv("AWS_CONFIG_FILE"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".aws", "config")
}

var roleAccount = regexp.MustCompile(`arn:aws[a-z-]*:iam::(\d{12}):`)

// ProfilesFor lists the profiles in the aws CLI config that log in to
// account: by sso_account_id, or the account of role_arn.
func ProfilesFor(account string) []string {
	f, err := os.Open(awsConfigPath())
	if err != nil {
		return nil
	}
	defer f.Close()
	return profilesFor(bufio.NewScanner(f), account)
}

func profilesFor(sc *bufio.Scanner, account string) []string {
	var out []string
	profile := ""
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			name := strings.TrimSpace(strings.Trim(line, "[]"))
			switch {
			case name == "default":
				profile = "default"
			case strings.HasPrefix(name, "profile "):
				profile = strings.TrimSpace(strings.TrimPrefix(name, "profile "))
			default:
				profile = "" // [sso-session x], [services x]
			}
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || profile == "" {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		match := key == "sso_account_id" && value == account
		if m := roleAccount.FindStringSubmatch(value); key == "role_arn" && m != nil && m[1] == account {
			match = true
		}
		if match && (len(out) == 0 || out[len(out)-1] != profile) {
			out = append(out, profile)
		}
	}
	sort.Strings(out)
	return out
}

// LoginFor finds an aws CLI login for account: the current environment
// when it is already there, else the first profile for it that works. It
// returns the extra environment to run the aws CLI with.
func LoginFor(ctx context.Context, account, region string) ([]string, error) {
	if got, err := (AWS{Region: region}).Account(ctx); err == nil && got == account {
		return nil, nil
	}
	profiles := ProfilesFor(account)
	if len(profiles) == 0 {
		return nil, fmt.Errorf("no profile for account %s in %s", account, awsConfigPath())
	}
	var lastErr error
	for _, p := range profiles {
		env := []string{"AWS_PROFILE=" + p}
		got, err := (AWS{Env: env, Region: region}).Account(ctx)
		if err == nil && got == account {
			return env, nil
		}
		if err != nil {
			lastErr = loginError(p, err)
		}
	}
	return nil, lastErr
}

// loginError says why a profile couldn't log in, and what to do.
func loginError(profile string, err error) error {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "sso") && (strings.Contains(msg, "expired") || strings.Contains(msg, "invalid")):
		return fmt.Errorf("your AWS login for profile %s has expired. Run aws sso login --profile %s", profile, profile)
	case strings.Contains(msg, "token") && strings.Contains(msg, "expired"):
		return fmt.Errorf("your AWS login for profile %s has expired. Log in again with that profile", profile)
	}
	return fmt.Errorf("can't log in with AWS profile %s: %w", profile, err)
}
