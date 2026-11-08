package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mxschmitt/playwright-go"
)

// This helper intentionally does not auto-solve or bypass VK CAPTCHA.
// It opens the provider page in a visible browser, waits for the user to
// complete the challenge, observes the page's own captchaNotRobot.check
// response, and returns only the resulting success_token to the parent ToTS
// process on stdout.

const tokenPrefix = "TOTS_CAPTCHA_SUCCESS_TOKEN="

type checkResult struct {
	Status       string `json:"status"`
	SuccessToken string `json:"success_token"`
	ErrorCode    int    `json:"error_code"`
	ErrorMsg     string `json:"error_msg"`
}

type vkResponse struct {
	Response checkResult `json:"response"`
}

func allowedCaptchaURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil {
		return false
	}
	if parsed.Port() != "" && parsed.Port() != "443" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	return host == "id.vk.ru" || host == "vk.ru" || host == "vk.com" ||
		strings.HasSuffix(host, ".vk.ru") || strings.HasSuffix(host, ".vk.com")
}

func parseSuccessToken(body []byte) string {
	var payload vkResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.Response.SuccessToken)
}

func ensurePlaywright() error {
	// playwright-go ships a small Go wrapper; its driver/browser payload is
	// installed on first use. This is user-visible setup, not a hidden solver.
	if err := playwright.Install(&playwright.RunOptions{Browsers: []string{"chromium"}}); err != nil {
		return fmt.Errorf("playwright install: %w", err)
	}
	return nil
}

func main() {
	var captchaURL string
	var timeoutSeconds int
	flag.StringVar(&captchaURL, "captcha_url", "", "VK captcha redirect URL")
	flag.IntVar(&timeoutSeconds, "timeout", 600, "maximum time to wait for manual completion, seconds")
	flag.Parse()

	if strings.TrimSpace(captchaURL) == "" {
		captchaURL = os.Getenv("TOTS_VK_CAPTCHA_URL")
	}
	captchaURL = strings.TrimSpace(strings.Trim(captchaURL, `"`))
	if !allowedCaptchaURL(captchaURL) {
		fmt.Fprintln(os.Stderr, "invalid or disallowed VK captcha URL")
		os.Exit(2)
	}
	if timeoutSeconds < 30 {
		timeoutSeconds = 30
	}

	log.Println("[captcha] opening VK challenge; complete it in the browser window")
	if err := ensurePlaywright(); err != nil {
		log.Fatalf("[captcha] browser setup failed: %v", err)
	}

	pw, err := playwright.Run()
	if err != nil {
		log.Fatalf("[captcha] could not start Playwright: %v", err)
	}
	defer pw.Stop()

	browser, err := pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(false),
		Args:     []string{"--start-maximized"},
	})
	if err != nil {
		log.Fatalf("[captcha] could not launch Chromium: %v", err)
	}
	defer browser.Close()

	context, err := browser.NewContext(playwright.BrowserNewContextOptions{
		Locale:   playwright.String("ru-RU"),
		Viewport: nil,
	})
	if err != nil {
		log.Fatalf("[captcha] could not create browser context: %v", err)
	}
	defer context.Close()

	page, err := context.NewPage()
	if err != nil {
		log.Fatalf("[captcha] could not create page: %v", err)
	}

	tokenCh := make(chan string, 1)
	page.On("response", func(response playwright.Response) {
		if !strings.Contains(response.URL(), "captchaNotRobot.check") {
			return
		}
		go func() {
			body, err := response.Body()
			if err != nil {
				return
			}
			token := parseSuccessToken(body)
			if token == "" {
				return
			}
			select {
			case tokenCh <- token:
			default:
			}
		}()
	})

	if _, err := page.Goto(captchaURL, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		Timeout:   playwright.Float(60000),
	}); err != nil {
		log.Fatalf("[captcha] navigation failed: %v", err)
	}

	timer := time.NewTimer(time.Duration(timeoutSeconds) * time.Second)
	defer timer.Stop()

	select {
	case token := <-tokenCh:
		// stdout is the machine-readable IPC channel consumed by ToTS.
		fmt.Printf("%s%s\n", tokenPrefix, token)
	case <-timer.C:
		log.Fatal("[captcha] timed out waiting for manual completion")
	}
}
