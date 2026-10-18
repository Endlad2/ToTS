package vkauth

import (
	"github.com/samosvalishe/tots/providers/vk/internal/browserprofile"
	"github.com/samosvalishe/tots/providers/vk/internal/personanet"

	tlsclient "github.com/bogdanfinn/tls-client"
)

func (c *Client) newTLSClient(profile browserprofile.Profile, jar tlsclient.CookieJar) (tlsclient.HttpClient, error) {
	return personanet.NewClient(profile, c.dialer, jar)
}
