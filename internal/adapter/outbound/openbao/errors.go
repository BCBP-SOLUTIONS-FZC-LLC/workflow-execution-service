package openbao

import "errors"

var (
	ErrUnauthorized = errors.New("openbao: unauthorized")

	ErrSecretNotFound = errors.New("openbao: secret not found")

	ErrUpstreamUnavailable = errors.New("openbao: upstream unavailable")

	ErrInvalidPath = errors.New("openbao: invalid secret path")
)
