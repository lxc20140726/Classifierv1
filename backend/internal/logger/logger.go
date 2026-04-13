package logger

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Writers holds the configured output writers for each log stream.
type Writers struct {
	// App is a multi-writer (stdout + rotating app.log) for runtime logs and
	// the Gin HTTP access log. Assign to gin.DefaultWriter before r.Use(gin.Logger()).
	App io.Writer
	// Audit is a rotating writer for audit.log (JSON Lines).
	Audit io.Writer
}

// Setup creates the log directory, opens rotating file writers, redirects the
// stdlib log package to write to both stdout and app.log, and returns Writers
// for callers to wire into Gin and AuditService.
//
// Files:
//
//	<logDir>/app.log   — runtime + HTTP access log (100 MB, 30 files, 30 days)
//	<logDir>/audit.log — audit JSON lines         (100 MB, 30 files, 90 days)
func Setup(logDir string) (*Writers, error) {
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}

	appLJ := &lumberjack.Logger{
		Filename:   filepath.Join(logDir, "app.log"),
		MaxSize:    100,
		MaxBackups: 30,
		MaxAge:     30,
		Compress:   true,
		LocalTime:  true,
	}

	auditLJ := &lumberjack.Logger{
		Filename:   filepath.Join(logDir, "audit.log"),
		MaxSize:    100,
		MaxBackups: 30,
		MaxAge:     90,
		Compress:   true,
		LocalTime:  true,
	}

	appWriter := io.MultiWriter(os.Stdout, appLJ)

	log.SetOutput(appWriter)
	log.SetFlags(log.LstdFlags)

	return &Writers{App: appWriter, Audit: auditLJ}, nil
}
