// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

require('dotenv').config()
const AES = require('./lib/aes.js')
const cluster = require('cluster')
require('colors')
const Config = require('./config.json')
const Helpers = require('./lib/helpers')
const MessageSigner = require('./lib/messageSigner.js')
const Rabbit = require('./lib/rabbit')
const request = require('request-promise-native')
const TurtleCoinUtils = require('turtlecoin-utils').CryptoNote
const util = require('util')

const cpuCount = require('os').cpus().length
const cryptoUtils = new TurtleCoinUtils()
const signer = new MessageSigner()
const topBlockUrl = Config.blockHeaderUrl + 'top'

function spawnNewWorker () {
  cluster.fork()
}

if (cluster.isMaster) {
  if (!process.env.NODE_ENV || process.env.NODE_ENV.toLowerCase() !== 'production') {
    Helpers.log('[WARNING] Node.js is not running in production mode. Consider running in production mode: export NODE_ENV=production'.yellow)
  }

  for (var cpuThread = 0; cpuThread < cpuCount; cpuThread++) {
    spawnNewWorker()
  }

  cluster.on('exit', (worker, code, signal) => {
    Helpers.log(util.format('worker %s died', worker.process.pid))
    spawnNewWorker()
  })
} else if (cluster.isWorker) {
  const crypto = new AES({ password: process.env.RABBIT_PRIVATE_ENCRYPTION_KEY || '' })

  /* Set up our connection to the public RabbitMQ server */
  const publicMQ = new Rabbit(
    process.env.RABBIT_PUBLIC_SERVER || 'localhost',
    process.env.RABBIT_PUBLIC_USERNAME || '',
    process.env.RABBIT_PUBLIC_PASSWORD || '',
    false
  )

  publicMQ.on('log', log => {
    Helpers.log(util.format('[RABBIT] %s', log))
  })

  publicMQ.on('connect', () => {
    Helpers.log(util.format('[RABBIT] connected to server at %s', process.env.RABBIT_PUBLIC_SERVER || 'localhost'))
  })

  publicMQ.on('disconnect', (error) => {
    Helpers.log(util.format('[RABBIT] lost connected to server: %s', error.toString()))
    cluster.worker.kill()
  })

  /* Set up our connection to the private RabbitMQ server */
  const privateMQ = new Rabbit(process.env.RABBIT_PRIVATE_SERVER || 'localhost', process.env.RABBIT_PRIVATE_USERNAME || '', process.env.RABBIT_PRIVATE_PASSWORD || '', false)
  privateMQ.on('log', log => {
    Helpers.log(util.format('[RABBIT] %s', log))
  })

  privateMQ.on('connect', () => {
    Helpers.log(util.format('[RABBIT] connected to server at %s', process.env.RABBIT_PRIVATE_SERVER || 'localhost'))
  })

  privateMQ.on('disconnect', (error) => {
    Helpers.log(util.format('[RABBIT] lost connected to server: %s', error.toString()))
    cluster.worker.kill()
  })

  publicMQ.on('message', (queue, message, payload) => {
    function run () {
      /* Generate a new address, nothing fancy going on here, just a brand new address */
      var newAddress = cryptoUtils.createNewAddress()
      const tmp = {}

      if (payload.privateViewKey) {
        const decodedAddress = cryptoUtils.decodeAddress(payload.address)
        const publicViewKey = cryptoUtils.privateKeyToPublicKey(payload.privateViewKey)

        if (publicViewKey !== decodedAddress.publicViewKey) throw new Error('derived public view key does not match address supplied')

        newAddress = {
          spend: {
            privateKey: false,
            publicKey: decodedAddress.publicSpendKey
          },
          view: {
            privateKey: payload.privateViewKey,
            publicKey: decodedAddress.publicViewKey
          },
          address: payload.address,
          paymentId: newAddress.spend.publicKey
        }
      }

      /* We need to go get some information about the current top block */
      return request({
        url: topBlockUrl,
        json: true
      })
        .then(topBlock => {
          tmp.topBlock = topBlock

          /* Now that we know what our scan height is, we can get moving */
          Helpers.log(util.format('Worker #%s: Created new wallet %s at height %s for %s', cluster.worker.id, newAddress.address, tmp.topBlock.height, message.properties.correlationId))

          /* Get a random set of keys for message signing */
          return signer.generateKeys()
        })
        .then(keys => {
          tmp.keys = keys

          /* Construct our payload for sending via the private RabbitMQ server
             to the workers that are actually scanning the wallets for incoming
             funds */
          const scanPayload = {
            wallet: newAddress,
            scanHeight: tmp.topBlock.height,
            maxHeight: (tmp.topBlock.height + Config.maximumScanBlocks),
            request: payload,
            privateKey: tmp.keys.privateKey,
            txs: [],
            viewOnly: false
          }

          if (newAddress.paymentId) scanPayload.viewOnly = true

          /* First, we encrypt the object that we are going to send to our queues */
          const encryptedPayload = { encrypted: crypto.encrypt(scanPayload) }

          /* Go ahead and send that message to the scanning queue */
          return privateMQ.sendToQueue(Config.queues.scan, encryptedPayload, { persistent: true })
        })
        .then(() => {
          /* Construct our response back to the public API -- non-sensitive information */
          const response = {
            address: newAddress.address,
            scanHeight: tmp.topBlock.height,
            maxHeight: (tmp.topBlock.height + Config.maximumScanBlocks),
            publicKey: tmp.keys.publicKey
          }

          if (newAddress.paymentId) {
            response.address = cryptoUtils.createIntegratedAddress(newAddress.address, newAddress.paymentId)
            response.paymentId = newAddress.paymentId
          }

          /* Go ahead and fire that information back to the public API for the related request */
          return publicMQ.reply(message, response)
        })
    }

    return run()
      .then(() => { return publicMQ.ack(message) })
      .catch(error => {
        Helpers.log(util.format('[INFO] Error encountered in worker %s: %s', cluster.worker.id, error.toString()))
        publicMQ.nack(message)
          .catch((error) => Helpers.log(util.format('Worker #%s: Could not nack message [%s]', cluster.worker.id, error.toString())))
      })
  })

  publicMQ.connect()
    .then(() => { return publicMQ.createQueue(Config.queues.new) })
    .then(() => { return privateMQ.connect() })
    .then(() => { return privateMQ.createQueue(Config.queues.scan) })
    .then(() => { return publicMQ.registerConsumer(Config.queues.new, 1) })

  Helpers.log(util.format('Worker #%s awaiting requests', cluster.worker.id))
}
