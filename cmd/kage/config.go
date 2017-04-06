package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/msales/kage/kage"
	"gopkg.in/alecthomas/kingpin.v2"
)

func readConfig() (*kage.Config, error) {
	var cmdConfig kage.Config
	var configFiles []string

	app := kingpin.New("kage", "Kafka metrics").Version(Version).DefaultEnvars()
	app.HelpFlag.Short('h')
	app.VersionFlag.Short('v')

	app.Flag("config", "The path to the JSON configuration file. This will be overriden by any command line arguemnts").StringsVar(&configFiles)

	app.Flag("log", "The type of log to use. Options: 'stdout', 'file'").EnumVar(&cmdConfig.Log, "stdout", "file")
	app.Flag("log-file", "The path to the file to log to. Only works when --log is set to file").StringVar(&cmdConfig.LogFile)
	app.Flag("log-level", "The log level to use. Options: 'debug', 'info', 'warn', 'error'").EnumVar(&cmdConfig.LogLevel, "debug", "info", "warn", "error")

	app.Flag("reporters", "The reporters to use. Options: 'influx', 'stdout'").EnumsVar(&cmdConfig.Reporters, "influx", "stdout")
	app.Flag("influx", "The DSN of the InfluxDB server to report to. Format: http://user:pass@ip:port/database'").StringVar(&cmdConfig.Influx.DSN)
	app.Flag("influx-metric", "The measurement name to report statistics under").StringVar(&cmdConfig.Influx.Metric)
	app.Flag("influx-tags", "Additional tags to add to the statistics.").PlaceHolder("KEY:VALUE").StringMapVar(&cmdConfig.Influx.Tags)

	app.Flag("addr", "The address to bind to for the http server").StringVar(&cmdConfig.Server.Address)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	config := defaultConfig()

	for _, path := range configFiles {
		fileConfig, err := readConfigFile(path)
		if err != nil {
			return nil, err
		}

		config = mergeConfig(config, fileConfig)
	}

	config = mergeConfig(config, &cmdConfig)

	return config, nil
}

// Reads a config file from the given path
func readConfigFile(path string) (*kage.Config, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := &kage.Config{}
	if err := json.Unmarshal(file, config); err != nil {
		return nil, err
	}

	return config, nil
}

func defaultConfig() *kage.Config {
	return &kage.Config{
		Log: "stdout",
		LogLevel: "info",
		Kafka: kage.KafkaConfig{
			Brokers: []string{"127.0.0.1:9092"},
		},
		Reporters: []string{"stdout"},
	}
}

func mergeConfig(a, b *kage.Config) *kage.Config {
	var result kage.Config = *a

	// Log
	if b.Log != "" {
		result.Log = b.Log
	}

	if b.LogFile != "" {
		result.LogFile = b.LogFile
	}

	if b.LogLevel != "" {
		result.LogLevel = b.LogLevel
	}

	// Kafka
	if len(b.Kafka.Brokers) > 0 {
		result.Kafka.Brokers = b.Kafka.Brokers
	}

	if len(b.Kafka.Ignore.Topics) > 0 {
		result.Kafka.Ignore.Topics = b.Kafka.Ignore.Topics
	}

	if len(b.Kafka.Ignore.Groups) > 0 {
		result.Kafka.Ignore.Groups = b.Kafka.Ignore.Groups
	}

	// Reporters
	if len(b.Reporters) > 0 {
		result.Reporters = b.Reporters
	}

	if b.Influx.DSN != "" {
		result.Influx.DSN = b.Influx.DSN
	}

	if b.Influx.Metric != "" {
		result.Influx.Metric = b.Influx.Metric
	}

	if len(b.Influx.Tags) > 0 {
		result.Influx.Tags = b.Influx.Tags
	}

	// Server
	if b.Server.Address != "" {
		result.Server.Address = b.Server.Address
	}

	return &result
}
