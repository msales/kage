# Kage

[![Go Report Card](https://goreportcard.com/badge/github.com/msales/kage)](https://goreportcard.com/report/github.com/msales/kage)
[![Build Status](https://travis-ci.org/msales/kage.svg?branch=master)](https://travis-ci.org/msales/kage)
[![Coverage Status](https://coveralls.io/repos/github/msales/kage/badge.svg?branch=master)](https://coveralls.io/github/msales/kage?branch=master)
[![GitHub release](https://img.shields.io/github/release/msales/kage.svg)](https://github.com/msales/kage/releases)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/msales/kage/master/LICENSE)

## Synopsis

kage (as in "Kafka AGEnt") reads Offset- and Lag metrics from Kafka and writes them to an InfluxDB.

## Motivation

When you're running Kafka you probably want to have monitoring as well.  
You can - of course - query the beans directly via JMX and work with that, but that requires another JVM that collects the data.  
If you're a java-shop anyway and have all that available - give it a try.  
We decided that we wanted to get the metrics straight out of Kafka and feed them into InfluxDB in a configurable way - and here we are now.

## Basic Installation

Grab a binary from the Releases or clone the repo and build it yourself.  
Fill the provided [sample configuration](example.json) with appropriate configuration for your setup, copy both files to the host you want to run kage on and run
```
kage --config=example.json

```

## Advanced Installation

There's systemd configuration magic in examples/systemd/.  
Put the files in the appropriate directories on your machine (in case of Debian/Ubuntu that should be /lib/systemd/system and /lib/systemd/system-generators), remember to chmod 0755 the generator, create /etc/kage/, run ```systemctl daemon-reload``` and then you should get one service per configuration-file in /etc/kage/.

## Contributors

We're supposed to tell you how to contribute to kage here.  
Since this is github: You know the drill - open issues, fork, create PRs, ...

## Todo

 * provide ansible-templates and examples
 * set up debian packaging
 * maybe provide minimal docker container

## License

MIT-License. As is. No warranties whatsoever. Mileage may vary. Batteries not included.
