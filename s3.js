'use strict'

const async = require('async')
const _ = require('lodash')
const Base = require('bfx-facs-base')
const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectsCommand
} = require('@aws-sdk/client-s3')
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner')
const { Upload } = require('@aws-sdk/lib-storage')

function client (conf, label) {
  const client = new S3Client(conf)

  return client
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
        const credentials = _.pick(
          this.conf,
          ['accessKeyId', 'secretAccessKey']
        )

        const {
          accessKeyId,
          secretAccessKey
        } = credentials

        const region = this.conf.region

        if (!accessKeyId || !secretAccessKey || !region) {
          return next(
            new Error('accessKeyId, secretAccessKey, or region missing in config')
          )
        }

        this.cli = client({ credentials, region })
        next(null)
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      next => {
        delete this.cli
        next()
      }
    ], cb)
  }

  async upload (params) {
    const command = new PutObjectCommand(params)
    return this.cli.send(command);
  }

  async uploadStream (params) {
    const req = new Upload({
      client: this.cli,
      params
    })
  
    return req.done()
  }

  async getSignedUrl (params) {
    const command = new GetObjectCommand(params)
    const url = await getSignedUrl(this.cli, command, { expiresIn: 3600 })
    return url
  }

  async deleteObjects (params) {
    const command = new DeleteObjectsCommand(params)
    return this.cli.send(command)
  }
}

module.exports = StoreFacility
