# Kage

[![Go Report Card](https://goreportcard.com/badge/github.com/msales/kage)](https://goreportcard.com/report/github.com/msales/kage)
[![Build Status](https://travis-ci.org/msales/kage.svg?branch=master)](https://travis-ci.org/msales/kage)
[![Coverage Status](https://coveralls.io/repos/github/msales/kage/badge.svg?branch=master)](https://coveralls.io/github/msales/kage?branch=master)
[![GitHub release](https://img.shields.io/github/release/msales/kage.svg)](https://github.com/msales/kage/releases)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/msales/kage/master/LICENSE)

## Synopsis

kage reads Offset- and Lagmetrics from Kafka and writes them to an InfluxDB.

## Motivation

When you're running Kafka you probably want to have monitoring as well.
You can - of course - query the beans directly via JMX and work with that, but that requires another JVM that collects the data.
If you're a java-shop anyway and have all that available - give it a try.
We decided that we wanted to get the metrics straight out of Kafka and feed them into InfluxDB in a configurable way - and here we are now.

## Installation

Grab a binary from the [Releases](releases) or clone the repo and build it yourself.
Fill the provided example.json with appropriate configuration for your setup, copy both files to the host you want to run kage on and run
```
kage --config=example.json

```

## Contributors

We're supposed to tell you how to contribute to kage here.
Since this is github: You know the drill - open issues, fork, create PRs, ...

## Todo

 * provide systemd-config-snippets
 * provide ansible-templates and examples
 * set up debian packaging

## License

MIT-License. As is. No warranties whatsoever. Mileage may vary. Batteries not included.
