package groxy

import (
	"errors"
)

var (
	ErrTimeout            = errors.New("Timed out before receiving response")
	ErrFailedToMakeHeader = errors.New("Failed to create binary header")
	ErrFailedToReadHeader = errors.New("Failed to read binary header")
	ErrNotGroxyMessage    = errors.New("Message was not a groxy message")
)
