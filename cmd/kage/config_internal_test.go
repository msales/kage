package main

import (
	"io/ioutil"
	"os"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestReadConfig_Cli(t *testing.T) {
	args := []string{
		"--log=file",
		"--reporters=stdout",
		"--reporters=influx",
		"--brokers=foo",
		"--brokers=bar",
		"--influx-tags=foo:bar",
		"--influx-tags=bat:baz",
	}

	config, err := readConfig(args)
	assert.NoError(t, err)

	assert.Equal(t, "file", config.Log)
	assert.Equal(t, []string{"stdout", "influx"}, config.Reporters)
	assert.Equal(t, map[string]string{"foo": "bar", "bat": "baz"}, config.Influx.Tags)
}

func TestReadConfig_CliAndFile(t *testing.T) {
	tf, err := ioutil.TempFile("", "kage")
	assert.NoError(t, err)

	tf.Write([]byte(`{"log-level":"warn"}`))
	tf.Close()
	defer os.Remove(tf.Name())

	args := []string{
		"--config=" + tf.Name(),
		"--log=file",
		"--reporters=stdout",
		"--reporters=influx",
		"--brokers=foo",
		"--brokers=bar",
		"--influx-tags=foo:bar",
		"--influx-tags=bat:baz",
	}

	config, err := readConfig(args)
	assert.NoError(t, err)

	assert.Equal(t, "file", config.Log)
	assert.Equal(t, "warn", config.LogLevel)
	assert.Equal(t, []string{"stdout", "influx"}, config.Reporters)
	assert.Equal(t, map[string]string{"foo": "bar", "bat": "baz"}, config.Influx.Tags)
}

func TestReadConfigFile(t *testing.T) {
	tf, err := ioutil.TempFile("", "kage")
	assert.NoError(t, err)

	tf.Write([]byte(`{"log":"file"}`))
	tf.Close()
	defer os.Remove(tf.Name())

	config, err := readConfigFile(tf.Name())
	assert.NoError(t, err)

	assert.Equal(t, "file", config.Log)
}

func TestReadConfigFile_BadPath(t *testing.T) {
	_, err := readConfigFile("/foo/bar")
	assert.Error(t, err)
}

func TestReadConfigFile_BadFile(t *testing.T) {
	tf, err := ioutil.TempFile("", "kage")
	assert.NoError(t, err)

	tf.Write([]byte(`{"log":1}`))
	tf.Close()
	defer os.Remove(tf.Name())

	_, err = readConfigFile(tf.Name())
	assert.Error(t, err)
}
