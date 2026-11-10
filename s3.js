'use strict'

const async = require('async')
const AWS = require('aws-sdk')
const Base = require('@bitfinex/bfx-facs-base')

function client (conf, label) {
  const s3 = new AWS.S3(conf)

  AWS.events.on('error', err => {
    console.error(label || 'generic', err)
  })

  return s3
}

class StoreFacility extends Base {
  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 'store-s3'
    this._hasConf = true

    this.init()
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        const conf = {}
        for (const k of  ['accessKeyId', 'secretAccessKey', 'region']) {
          if (this.conf.hasOwnProperty(k)) {
            conf[k] = this.conf[k]
          }
        }

        const {
          accessKeyId,
          secretAccessKey,
          region
        } = conf

        if (!accessKeyId || !secretAccessKey || !region) {
          return next(
            new Error('accessKeyId, secretAccessKey, or region missing in config')
          )
        }

        this.cli = client(conf)
        next(null)
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      next => {
        // AWS.events.off('error')  hmm, no off in API, possible leak
        delete this.cli
        next()
      }
    ], cb)
  }
}

module.exports = StoreFacility
