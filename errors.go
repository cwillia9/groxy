package groxy

import (
	"errors"
)

var ErrTimeout = errors.New("Timed out before receiving response")
var ErrFailedToMakeHeader = errors.New("Failed to create binary header")
var ErrFailedToReadHeader = errors.New("Failed to read binary header")
var ErrNotGroxyMessage = errors.New("Message was not a groxy message")
