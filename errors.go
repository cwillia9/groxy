package groxy

import (
	"errors"
)

var ErrTimeout = errors.New("Timed out before receiving response")
