package error

import (
	"fmt"
)

func Errorf(errorCode string, err error) error {
	return fmt.Errorf("%s: %w", errorCode, err)
}
