'use strict'

const async = require('async')
const _ = require('lodash')
const S3 = require('aws-sdk/clients/s3');

const Facility = require('./base')

function client (conf, label) {

  let s3 = new S3(conf)

  s3.evets.on('error', err => {
    console.error(label || 'generic', err)
  })

  return s3
}

class S3Facility extends Facility {
  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 's3'
    this._hasConf = true

    this.init()
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        this.cli = client(_.pick(
          this.conf,
          ['accessKeyId', 'secretAccessKey', 'region']
        ))
        next()
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      next => {
        this.cli.off('error')
        delete this.cli
        next()
      }
    ], cb)
  }
}

module.exports = S3Facility
