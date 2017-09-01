package main

import (
	"encoding/json"
	"io/ioutil"

	"github.com/msales/kage"
	"gopkg.in/alecthomas/kingpin.v2"
)

func readConfig(args []string) (*kage.Config, error) {
	var cmdConfig kage.Config
	var configFiles []string

	cmdConfig.Influx.Tags = map[string]string{}

	app := kingpin.New("kage", "Monitor metrics").Version(Version).DefaultEnvars()
	app.HelpFlag.Short('h')
	app.VersionFlag.Short('v')

	app.Flag("config", "The path to the JSON configuration file. This will be overridden by any command line arguments").StringsVar(&configFiles)

	app.Flag("log", "The type of log to use. Options: 'stdout', 'file'").EnumVar(&cmdConfig.Log, "stdout", "file")
	app.Flag("log-file", "The path to the file to log to. Only works when --log is set to file").StringVar(&cmdConfig.LogFile)
	app.Flag("log-level", "The log level to use. Options: 'debug', 'info', 'warn', 'error'").EnumVar(&cmdConfig.LogLevel, "debug", "info", "warn", "error")

	app.Flag("brokers", "The kafka seed brokers connect to, Format: 'ip:port'").StringsVar(&cmdConfig.Kafka.Brokers)
	app.Flag("ignore-topics", "The kafka topic patterns to ignore. This may contain wildcards").StringsVar(&cmdConfig.Kafka.Ignore.Topics)
	app.Flag("ignore-groups", "The kafka consumer group patterns to ignore. This may contain wildcards").StringsVar(&cmdConfig.Kafka.Ignore.Groups)

	app.Flag("reporters", "The reporters to use. Options: 'influx', 'stdout'").EnumsVar(&cmdConfig.Reporters, "influx", "stdout")
	app.Flag("influx", "The DSN of the InfluxDB server to report to. Format: 'http://user:pass@ip:port/database'").StringVar(&cmdConfig.Influx.DSN)
	app.Flag("influx-metric", "The measurement name to report statistics under").StringVar(&cmdConfig.Influx.Metric)
	app.Flag("influx-policy", "The retention policy to report statistics under").Default("").StringVar(&cmdConfig.Influx.Policy)
	app.Flag("influx-tags", "Additional tags to add to the statistics").PlaceHolder("KEY:VALUE").StringMapVar(&cmdConfig.Influx.Tags)

	app.Flag("addr", "The address to bind to for the http server").StringVar(&cmdConfig.Server.Address)

	kingpin.MustParse(app.Parse(args))

	config := DefaultConfig()

	for _, path := range configFiles {
		fileConfig, err := readConfigFile(path)
		if err != nil {
			return nil, err
		}

		config = kage.MergeConfig(config, fileConfig)
	}

	config = kage.MergeConfig(config, &cmdConfig)

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

// DefaultConfig creates a default instance of the configuration.
func DefaultConfig() *kage.Config {
	return &kage.Config{
		Log:      "stdout",
		LogLevel: "info",
		Kafka: kage.KafkaConfig{
			Ignore: kage.KafkaIgnoreConfig{
				Topics: []string{},
				Groups: []string{},
			},
		},
		Reporters: []string{"stdout"},
		Influx: kage.InfluxConfig{
			Tags: map[string]string{},
		},
	}
}
