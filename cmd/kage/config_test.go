package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
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
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if config.Log != "file" {
		t.Fatalf("expected file; got %v", config.Log)
	}

	if !reflect.DeepEqual(config.Reporters, []string{"stdout", "influx"}) {
		t.Fatalf("expected stdout,influx; got %v", config.Reporters)
	}

	if !reflect.DeepEqual(config.Influx.Tags, map[string]string{"foo": "bar", "bat": "baz"}) {
		t.Fatalf("expected foo:bar,bat:baz; got %v", config.Influx.Tags)
	}
}

func TestReadConfig_CliAndFile(t *testing.T) {
	tf, err := ioutil.TempFile("", "kage")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
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
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if config.Log != "file" {
		t.Fatalf("expected file; got %v", config.Log)
	}

	if config.LogLevel != "warn" {
		t.Fatalf("expected warn; got %v", config.LogLevel)
	}

	if !reflect.DeepEqual(config.Reporters, []string{"stdout", "influx"}) {
		t.Fatalf("expected stdout,influx; got %v", config.Reporters)
	}

	if !reflect.DeepEqual(config.Influx.Tags, map[string]string{"foo": "bar", "bat": "baz"}) {
		t.Fatalf("expected foo:bar,bat:baz; got %v", config.Influx.Tags)
	}
}

func TestReadConfigFile(t *testing.T) {
	tf, err := ioutil.TempFile("", "kage")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	tf.Write([]byte(`{"log":"file"}`))
	tf.Close()
	defer os.Remove(tf.Name())

	config, err := readConfigFile(tf.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	if config.Log != "file" {
		t.Fatalf("config not parsed correctly: %v", config)
	}
}

func TestReadConfigFile_BadPath(t *testing.T) {
	_, err := readConfigFile("/foo/bar")
	if err == nil {
		t.Fatal("should have error")
	}
}

func TestReadConfigFile_BadFile(t *testing.T) {
	tf, err := ioutil.TempFile("", "kage")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	tf.Write([]byte(`{"log":1}`))
	tf.Close()
	defer os.Remove(tf.Name())

	_, err = readConfigFile(tf.Name())
	if err == nil {
		t.Fatal("should have error")
	}
}
