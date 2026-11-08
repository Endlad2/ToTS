package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mxschmitt/playwright-go"
)

const stealthJS = `(() => {
  try { Object.defineProperty(Navigator.prototype, 'webdriver', { get: () => undefined, configurable: true }); } catch (e) {}
  if (!window.chrome) window.chrome = {};
  window.chrome.runtime = window.chrome.runtime || {};
  window.chrome.app = window.chrome.app || { isInstalled: false };
  window.chrome.csi = window.chrome.csi || function () { return {}; };
  window.chrome.loadTimes = window.chrome.loadTimes || function () { return {}; };
  try {
    const fakePlugins = [
      { name: 'PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
      { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
      { name: 'Chromium PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
    ];
    Object.defineProperty(Navigator.prototype, 'plugins', {
      get: () => {
        const arr = fakePlugins.map(p => Object.assign(Object.create(Plugin.prototype), p));
        arr.item = i => arr[i];
        arr.namedItem = n => arr.find(p => p.name === n);
        return arr;
      }, configurable: true,
    });
  } catch (e) {}
  try {
    Object.defineProperty(Navigator.prototype, 'languages', { get: () => ['ru-RU','ru','en-US','en'], configurable: true });
    Object.defineProperty(Navigator.prototype, 'language', { get: () => 'ru-RU', configurable: true });
  } catch (e) {}
  try {
    Object.defineProperty(Navigator.prototype, 'hardwareConcurrency', { get: () => 8, configurable: true });
    Object.defineProperty(Navigator.prototype, 'deviceMemory', { get: () => 8, configurable: true });
  } catch (e) {}
  try {
    const gp = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function (p) {
      if (p === 37445) return 'Intel Inc.';
      if (p === 37446) return 'Intel Iris OpenGL Engine';
      return gp.call(this, p);
    };
    const gp2 = WebGL2RenderingContext.prototype.getParameter;
    WebGL2RenderingContext.prototype.getParameter = function (p) {
      if (p === 37445) return 'Intel Inc.';
      if (p === 37446) return 'Intel Iris OpenGL Engine';
      return gp2.call(this, p);
    };
  } catch (e) {}

  window.__captchaSuccessToken = window.__captchaSuccessToken || null;
  const grab = (json) => {
    const t = json?.response?.success_token;
    if (t) { console.log('[captcha] success_token:', t); window.__captchaSuccessToken = t; }
    return json;
  };
  const of = window.fetch;
  window.fetch = function (...a) {
    const p = of.apply(this, a);
    const u = typeof a[0] === 'string' ? a[0] : a[0]?.url || '';
    if (u.includes('captchaNotRobot.check')) p.then(r => r.clone().json()).then(grab).catch(() => {});
    return p;
  };
  const oo = XMLHttpRequest.prototype.open, os = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function (m, u, ...r) { this._url = u; return oo.call(this, m, u, ...r); };
  XMLHttpRequest.prototype.send = function (...a) {
    if ((this._url || '').includes('captchaNotRobot.check')) {
      this.addEventListener('load', () => { try { grab(JSON.parse(this.responseText)); } catch {} });
    }
    return os.apply(this, a);
  };
})();`

type checkResult struct {
	Status       string `json:"status"`
	SuccessToken string `json:"success_token"`
	Redirect     string `json:"redirect"`
	ShowType     string `json:"show_captcha_type"`
	ErrorCode    int    `json:"error_code"`
	ErrorMsg     string `json:"error_msg"`
}
type vkResponse struct{ Response checkResult `json:"response"` }

var (
	tokenOnce sync.Once
	tokenVal  string
	tokenCh   = make(chan string, 4)
	statusCh  = make(chan checkResult, 8)
)

func captureBody(b []byte) {
	var v vkResponse
	if json.Unmarshal(b, &v) != nil {
		return
	}
	r := v.Response
	if r.SuccessToken != "" {
		tokenOnce.Do(func() { tokenVal = r.SuccessToken; tokenCh <- r.SuccessToken })
	}
	select {
	case statusCh <- r:
	default:
	}
}

func ensurePlaywright() error {
	log.Println("[setup] ensuring playwright driver + chromium (idempotent)...")
	if err := playwright.Install(&playwright.RunOptions{Browsers: []string{"chromium"}}); err != nil {
		return fmt.Errorf("playwright install: %w", err)
	}
	log.Println("[setup] OK")
	return nil
}

func rnd(a, b int) int {
	if b <= a {
		return a
	}
	return a + rand.Intn(b-a)
}

func humanMove(page playwright.Page, tx, ty float64) {
	sx := tx - float64(rnd(200, 400))
	sy := ty - float64(rnd(80, 200))
	_ = page.Mouse().Move(sx, sy)
	steps := rnd(8, 16)
	for i := 1; i <= steps; i++ {
		f := float64(i) / float64(steps)
		x := sx + (tx-sx)*f + float64(rnd(-2, 3))
		y := sy + (ty-sy)*f + float64(rnd(-2, 3))
		_ = page.Mouse().Move(x, y)
		time.Sleep(time.Duration(rnd(8, 22)) * time.Millisecond)
	}
}

func main() {
	var captchaURL string
	var headful, keepOpen bool
	var slowmo int
	var channel string
	flag.StringVar(&captchaURL, "captcha_url", "", "URL of the captcha page")
	flag.BoolVar(&headful, "headful", true, "show browser window (default true)")
	flag.BoolVar(&keepOpen, "keep-open", false, "leave browser open after attempt (debug)")
	flag.IntVar(&slowmo, "slowmo", 0, "slow-motion ms for every Playwright action")
	flag.StringVar(&channel, "channel", "", "browser channel: '', 'chrome', 'msedge'")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	captchaURL = strings.TrimPrefix(captchaURL, "=")
	captchaURL = strings.Trim(captchaURL, `"`)
	if captchaURL == "" {
		fmt.Fprintln(os.Stderr, "error: --captcha_url is required")
		flag.Usage()
		os.Exit(2)
	}

	if err := ensurePlaywright(); err != nil {
		log.Fatalf("setup failed: %v", err)
	}

	launchOpts := playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(!headful),
		Args: []string{
			"--disable-blink-features=AutomationControlled",
			"--no-sandbox",
			"--disable-dev-shm-usage",
			"--start-maximized",
		},
	}
	if channel != "" {
		launchOpts.Channel = playwright.String(channel)
	}
	if slowmo > 0 {
		launchOpts.SlowMo = playwright.Float(float64(slowmo))
	}

	pw, err := playwright.Run()
	if err != nil {
		log.Fatalf("could not start playwright: %v", err)
	}
	defer pw.Stop()

	browser, err := pw.Chromium.Launch(launchOpts)
	if err != nil {
		log.Fatalf("could not launch browser: %v", err)
	}
	defer func() {
		if keepOpen {
			log.Println("[keep-open] press Ctrl+C to exit; browser left open")
			select {}
		}
		browser.Close()
	}()

	context, err := browser.NewContext(playwright.BrowserNewContextOptions{
		UserAgent:         playwright.String("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
		Locale:            playwright.String("ru-RU"),
		TimezoneId:        playwright.String("Europe/Moscow"),
		ColorScheme:       playwright.ColorSchemeLight,
		Viewport:          nil,
		DeviceScaleFactor: playwright.Float(1),
	})
	if err != nil {
		log.Fatalf("could not create context: %v", err)
	}
	defer context.Close()

	if err := context.AddInitScript(playwright.Script{Content: playwright.String(stealthJS)}); err != nil {
		log.Fatalf("add init script: %v", err)
	}

	page, err := context.NewPage()
	if err != nil {
		log.Fatalf("new page: %v", err)
	}
	defer page.Close()

	page.On("console", func(m playwright.ConsoleMessage) {
		t := m.Text()
		if strings.Contains(t, "[captcha]") {
			fmt.Println(t)
		}
	})
	page.On("response", func(r playwright.Response) {
		if !strings.Contains(r.URL(), "captchaNotRobot.check") {
			return
		}
		log.Printf("[resp] %d", r.Status())
		go func() {
			if b, err := r.Body(); err == nil {
				log.Printf("[body] %s", string(b))
				captureBody(b)
			}
		}()
	})

	log.Printf("[nav] %s", captchaURL)
	if _, err := page.Goto(captchaURL, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		Timeout:   playwright.Float(60000),
	}); err != nil {
		log.Fatalf("goto: %v", err)
	}

	loc := page.Locator("#not-robot-captcha-checkbox")
	if err := loc.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateAttached,
		Timeout: playwright.Float(20000),
	}); err != nil {
		log.Fatalf("checkbox not found: %v", err)
	}

	preDelay := time.Duration(rnd(1500, 3500)) * time.Millisecond
	log.Printf("[wait] human delay %s", preDelay)
	time.Sleep(preDelay)

	box, err := loc.BoundingBox()
	if err != nil || box == nil {
		log.Printf("[warn] no bounding box: %v", err)
	} else {
		tx := box.X + box.Width/2
		ty := box.Y + box.Height/2
		log.Printf("[mouse] moving to (%.0f, %.0f)", tx, ty)
		humanMove(page, tx, ty)
		time.Sleep(time.Duration(rnd(120, 380)) * time.Millisecond)
		if err := page.Mouse().Down(); err != nil {
			log.Printf("[mouse] down: %v", err)
		}
		time.Sleep(time.Duration(rnd(40, 120)) * time.Millisecond)
		if err := page.Mouse().Up(); err != nil {
			log.Printf("[mouse] up: %v", err)
		}
	}

	// Wait for exactly ONE captchaNotRobot.check response. No reload, no retry.
	// Reloading burns rate limit; if VK says BOT we exit with an error, not retry.
	select {
	case t := <-tokenCh:
		fmt.Printf("[captcha] success_token: %s\n", t)
		return
	case <-time.After(15 * time.Second):
	}

	// One fallback click only if the first click produced NO network response at all.
	select {
	case s := <-statusCh:
		// VK responded — do NOT click again; that could trigger limit.
		log.Printf("[status] VK response: %+v", s)
		if s.Status == "BOT" || s.SuccessToken == "" {
			log.Fatalf("VK rejected fingerprint (status=%q). Not retrying to avoid rate limit. "+
				"Try --channel=chrome, headful on a real desktop, and a fresh session_token.", s.Status)
		}
		log.Fatalf("captcha not solved: %+v", s)
	default:
		// No network response — click may not have registered. Try label click once.
		log.Println("[fallback] no network response to first click; one label click")
		target := page.Locator(`label:has(#not-robot-captcha-checkbox)`).First()
		if n, _ := target.Count(); n > 0 {
			_ = target.Click(playwright.LocatorClickOptions{Timeout: playwright.Float(5000), Force: playwright.Bool(true)})
		}
		select {
		case t := <-tokenCh:
			fmt.Printf("[captcha] success_token: %s\n", t)
			return
		case s := <-statusCh:
			log.Printf("[status] VK response: %+v", s)
			log.Fatalf("VK rejected: %+v. Not retrying (rate limit).", s)
		case <-time.After(10 * time.Second):
			log.Fatal("no response from captchaNotRobot.check after two clicks")
		}
	}
}
