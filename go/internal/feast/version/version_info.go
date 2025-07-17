package version

import (
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/rs/zerolog/log"
	"os"
	"runtime"
	"strings"
)

var (
	Version    = "dev"
	BuildTime  = "unknown"
	CommitHash = "none"
	GoVersion  = runtime.Version()
	ServerType = "none"
)

type Info struct {
	Version    string `json:"version"`
	BuildTime  string `json:"build_time"`
	CommitHash string `json:"commit_hash"`
	GoVersion  string `json:"go_version"`
	ServerType string `json:"server_type"`
}

func GetVersionInfo() *Info {
	return &Info{
		Version:    Version,
		BuildTime:  BuildTime,
		CommitHash: CommitHash,
		GoVersion:  GoVersion,
		ServerType: ServerType,
	}
}

func PublishVersionInfoToDatadog() {
	if strings.ToLower(os.Getenv("ENABLE_DATADOG_TRACING")) == "true" {
		if statsdHost, ok := os.LookupEnv("DD_AGENT_HOST"); ok {
			var client, err = statsd.New(fmt.Sprintf("%s:8125", statsdHost))
			if err != nil {
				log.Error().Err(err).Msg("Failed to connect to statsd")
				return
			}
			info := GetVersionInfo()
			tags := []string{
				"feast_version:" + info.Version,
				"build_time:" + info.BuildTime,
				"commit_hash:" + info.CommitHash,
				"go_version:" + info.GoVersion,
				"server_type:" + info.ServerType,
			}
			err = client.Gauge("featureserver.heartbeat", 1, tags, 1)
			if err != nil {
				log.Error().Err(err).Msg("Failed to publish feature server heartbeat info to datadog")
			}
		} else {
			log.Info().Msg("DD_AGENT_HOST environment variable is not set, skipping publishing version info to Datadog")
		}
	}
}
