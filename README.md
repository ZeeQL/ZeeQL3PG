<h2>PostgreSQL Adaptor for ZeeQL3
  <img src="http://zeezide.com/img/ZeeQLIcon1024-QL.svg"
       align="right" width="128" height="128" />
</h2>

![Swift4.2](https://img.shields.io/badge/swift-4.2-blue.svg)
![Swift5](https://img.shields.io/badge/swift-5-blue.svg)
![macOS](https://img.shields.io/badge/os-macOS-green.svg?style=flat)
![tuxOS](https://img.shields.io/badge/os-tuxOS-green.svg?style=flat)
![Travis](https://travis-ci.org/ZeeQL/ZeeQL3PG.svg?branch=develop)

This library contains a ZeeQL database adaptor for
[PostgreSQL](https://www.postgresql.org).

Note: If your Swift code is running within Apache via
[mod_swift](http://mod-swift.org)
or
[ApacheExpress](http://apacheexpress.io/),
you may rather want to use mod_dbd.
mod_dbd adaptors are provided by the
ZeeQL3Apache
package.


## Installing libpq

To operate, ZeeQL3PG currently requires libpq. You can get it on macOS via
[Homebrew](https://brew.sh):

    brew install postgresql

and on Ubuntu using

    apt-get install libpq-dev

### Documentation

ZeeQL documentation can be found at:
[docs.zeeql.io](http://docs.zeeql.io/).

### Who

**ZeeQL** is brought to you by
[ZeeZide](http://zeezide.de).
We like feedback, GitHub stars, cool contract work,
presumably any form of praise you can think of.

There is a `#zeeql` channel on the [Noze.io Slack](http://slack.noze.io).
