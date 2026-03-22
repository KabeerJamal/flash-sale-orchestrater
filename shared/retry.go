package shared

import (
	"log/slog"
	"time"
)

func RetryExternal(attempts int, fn func() error) error {
	var err error
	for i := range attempts {
		err = fn()
		if err == nil {
			return nil
		}
		slog.Error("External call failed", "attempt", i+1, "error", err)
		time.Sleep(time.Second * time.Duration(i+1))
	}
	return err
}
