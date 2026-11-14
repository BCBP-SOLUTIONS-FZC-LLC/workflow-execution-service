package main

import "crypto/rand"

const fallbackConsumerName = "connector-worker"

func resolveConsumerName(configured string, hostname func() (string, error)) string {
	if configured != "" {
		return configured
	}
	if host, err := hostname(); err == nil && host != "" {
		return host
	}
	return fallbackConsumerName + "-" + rand.Text()[:12]
}
