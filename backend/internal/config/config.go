package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

// Config holds all runtime configuration loaded from environment variables.
type Config struct {
	SourceDir        string
	TargetDir        string
	DeleteStagingDir string
	ConfigDir        string
	LogDir           string
	Port             string
	TZ               string
	MaxConcurrency   int
}

type DeploymentPaths struct {
	DefaultScanRoot   string
	DefaultOutputRoot string
	DeleteStagingDir  string
}

// Load reads configuration from environment variables with sane defaults.
func Load() *Config {
	maxConcurrency := 4
	if v := os.Getenv("MAX_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConcurrency = n
		}
	}

	defaults := defaultPathDefaults()
	configDir := getEnv("CONFIG_DIR", defaults.ConfigDir)

	return &Config{
		SourceDir:        getEnv("SOURCE_DIR", defaults.SourceDir),
		TargetDir:        getEnv("TARGET_DIR", defaults.TargetDir),
		DeleteStagingDir: getEnv("DELETE_STAGING_DIR", defaults.DeleteStagingDir),
		ConfigDir:        configDir,
		LogDir:           getEnv("LOG_DIR", filepath.Join(configDir, "logs")),
		Port:             getEnv("PORT", "8080"),
		TZ:               getEnv("TZ", "Asia/Shanghai"),
		MaxConcurrency:   maxConcurrency,
	}
}

func (c *Config) DeploymentPaths() DeploymentPaths {
	if c == nil {
		return DeploymentPaths{}
	}
	return DeploymentPaths{
		DefaultScanRoot:   c.SourceDir,
		DefaultOutputRoot: c.TargetDir,
		DeleteStagingDir:  c.DeleteStagingDir,
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return defaultVal
}

type pathDefaults struct {
	SourceDir        string
	TargetDir        string
	DeleteStagingDir string
	ConfigDir        string
}

func defaultPathDefaults() pathDefaults {
	// Keep Linux defaults stable for container/NAS deployments.
	if runtime.GOOS == "linux" {
		return pathDefaults{
			SourceDir:        "/data/source",
			TargetDir:        "/data/target",
			DeleteStagingDir: "/data/delete_staging",
			ConfigDir:        "/data/config",
		}
	}

	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		homeDir = "."
	}
	baseDir := filepath.Join(homeDir, ".classifier")

	return pathDefaults{
		SourceDir:        filepath.Join(baseDir, "source"),
		TargetDir:        filepath.Join(baseDir, "target"),
		DeleteStagingDir: filepath.Join(baseDir, "delete_staging"),
		ConfigDir:        filepath.Join(baseDir, "config"),
	}
}
