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
const packageInfo = require('../package.json')
const Rabbit = require('./lib/rabbit')
const request = require('request-promise-native')
const TurtleCoinUtils = require('turtlecoin-utils').CryptoNote
const util = require('util')

const cpuCount = require('os').cpus().length
const cryptoUtils = new TurtleCoinUtils()

const userAgent = util.format('%s/%s', packageInfo.name, packageInfo.version)

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
  const privateMQ = new Rabbit(
    process.env.RABBIT_PRIVATE_SERVER || 'localhost',
    process.env.RABBIT_PRIVATE_USERNAME || '',
    process.env.RABBIT_PRIVATE_PASSWORD || '',
    false
  )

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

  privateMQ.on('message', (queue, message, payload) => {
    function run () {
      return new Promise((resolve, reject) => {
        /* If the payload is encrypted, we need to decrypt it */
        if (payload.encrypted) {
          payload = crypto.decrypt(payload.encrypted)
        }

        Helpers.log(util.format('Scanning for transactions to [%s]', payload.wallet.address))

        var confirmationsRequired = (typeof payload.request.confirmations !== 'undefined') ? payload.request.confirmations : Config.defaultConfirmations

        /* If someone somehow manages to send us through a request
           with a very long number of confirmations we could end
           up with a job stuck in our queue forever */
        if (confirmationsRequired > Config.maximumConfirmations) {
          confirmationsRequired = Config.maximumConfirmations
        }

        /* Let's get some basic block inofmration regarding our wallet */
        request({
          url: Config.blockHeaderUrl + 'top',
          json: true,
          headers: {
            'User-Agent': userAgent
          }
        })
          .then(topBlock => {
            payload.topBlock = topBlock

            /* If we're at the same block height as when the request was
               created, then there's 0% chance that our transaction has
               occurred */
            if (topBlock.height === payload.scanHeight) {
              return reject(new Error('Blockchain is at the same height as when the request was created'))
            }

            /* Let's go get blockchain transactional data so we can scan through it */

            return request({
              url: Config.syncUrl,
              json: true,
              method: 'POST',
              body: {
                scanHeight: payload.scanHeight,
                blockCount: (Config.maximumScanBlocks + 1)
              },
              headers: {
                'User-Agent': userAgent
              }
            })
          })
          .then(syncData => {
            /* Set up basic storage for some wallet information */
            payload.walletOutputs = []
            payload.totalAmount = 0
            payload.fundsFoundInBlock = 0

            /* Loop through the returned blocks */
            for (var i = 0; i < syncData.length; i++) {
              var block = syncData[i]

              /* Loop through transactions in the block */
              for (var j = 0; j < block.transactions.length; j++) {
                var transaction = block.transactions[j]

                /* Check to see if any of the outputs in the transaction belong to us */
                var outputs = cryptoUtils.scanTransactionOutputs(transaction.publicKey, transaction.outputs, payload.wallet.view.privateKey, payload.wallet.spend.publicKey, payload.wallet.spend.privateKey)

                /* If we found outputs, we need to store the top block height we found
                   the funds in so we know where to start our confirmation check from */
                if (outputs.length !== 0 && block.height > payload.fundsFoundInBlock) {
                  payload.fundsFoundInBlock = block.height
                }

                /* Loop through any found outputs and start tallying them up */
                for (var l = 0; l < outputs.length; l++) {
                  payload.totalAmount += outputs[l].amount
                  payload.walletOutputs.push(outputs[l])
                }
              }
            }
          })
          .then(() => {
            /* Did we find some outputs for us? */
            if (payload.walletOutputs.length > 0) {
              /* Did we find all the funds we requested and do we have the required confirmations? */
              if (payload.totalAmount >= payload.request.amount && (payload.topBlock.height - payload.fundsFoundInBlock) >= confirmationsRequired) {
                /* Congrats, we found all the funds that we requested and we're ready
                   to send them on. Generate the response for sending it back to the
                   requestor to let them know that funds were received (still incomplete) */
                const goodResponse = {
                  address: payload.wallet.address,
                  amount: payload.totalAmount,
                  status: 100, // Continue
                  request: payload.request,
                  privateKey: payload.privateKey
                }

                /* Stick our funds in our payload and clean up our stack */
                payload.funds = payload.walletOutputs
                const totalAmount = payload.totalAmount
                delete payload.walletOutputs
                delete payload.totalAmount
                delete payload.fundsFoundInBlock

                /* Encrypt our payload */
                const encryptedPayload = { encrypted: crypto.encrypt(payload) }

                Helpers.log(util.format('[INFO] Worker #%s found %s for [%s] and is forwarding request to send workers', cluster.worker.id, totalAmount, payload.wallet.address))

                /* Signal to the workers who send the funds to their real destination that things are ready */
                privateMQ.sendToQueue(Config.queues.send, encryptedPayload, { persistent: true })
                  .then(() => { return publicMQ.sendToQueue(Config.queues.complete, goodResponse, { persistent: true }) })
                  .then(() => { return privateMQ.ack(message) })
                  .then(() => { return resolve() })
              } else if (payload.totalAmount >= payload.request.amount) {
                /* We found all the funds we need, but we don't have enough confirmations yet */

                var confirmationsRemaining = confirmationsRequired - (payload.topBlock.height - payload.fundsFoundInBlock)
                if (confirmationsRemaining < 0) {
                  confirmationsRemaining = 0
                }

                /* We need to let everyone know that we found their monies but we need more confirmations */
                const waitingForConfirmations = {
                  address: payload.wallet.address,
                  amount: payload.totalAmount,
                  confirmationsRemaining: confirmationsRemaining,
                  status: 102,
                  request: payload.request,
                  privateKey: payload.privateKey
                }

                Helpers.log(util.format('[INFO] Worker #%s found %s for [%s] but is awaiting confirmations. %s blocks to go', cluster.worker.id, payload.totalAmount, payload.wallet.address, (confirmationsRequired - (payload.topBlock.height - payload.fundsFoundInBlock))))

                /* Send the notice back to the requestor, then nack the message (so it is tried again later,
                   and then finally resolve out */
                publicMQ.sendToQueue(Config.queues.complete, waitingForConfirmations, { persistent: true })
                  .then(() => { return privateMQ.nack(message) })
                  .then(() => { return resolve() })
              } else if (payload.topBlock.height > payload.maxHeight && (payload.topBlock.height - payload.fundsFoundInBlock) >= confirmationsRequired) {
                /* We found funds but it's not at least the amount that we requested and unfortunately
                   we've also ran out of time to look for more. Build a response that we can send back
                   to the requestor to let them know that we've received something, but not what they
                   requested, their request has timed out, and what we did receive will be on its way
                   shortly */

                const partialResponse = {
                  address: payload.wallet.address,
                  status: 206, // Partial Content (aka Partial Payment)
                  request: payload.request,
                  privateKey: payload.privateKey
                }

                /* Stick our funds in our payload */
                payload.funds = payload.walletOutputs
                const totalAmount = payload.totalAmount
                delete payload.walletOutputs
                delete payload.totalAmount
                delete payload.fundsFoundInBlock

                /* First we encrypt the object that we are going to send to our queues */
                const encryptedPayload = { encrypted: crypto.encrypt(payload) }

                Helpers.log(util.format('[INFO] Worker #%s found %s for [%s] and is forwarding request to send workers', cluster.worker.id, totalAmount, payload.wallet.address))

                /* Send the notice back to the requestor, then act the messages as we're done here */
                publicMQ.sendToQueue(Config.queues.complete, partialResponse, { persistent: true })
                  .then(() => { return privateMQ.sendToQueue(Config.queues.send, encryptedPayload, { persistent: true }) })
                  .then(() => { return privateMQ.ack(message) })
                  .then(() => { return resolve() })
              } else {
                /* We found some fund but not the amount that we are looking for and we still have
                   time to look for more before considering the request complete */

                var blocksRemaining = (payload.maxHeight - payload.topBlock.height)
                if (blocksRemaining < 0) {
                  blocksRemaining = 0
                }

                /* We need to let everyone know that we found some monies but we need more */
                const waitingForConfirmationsNotEnough = {
                  address: payload.wallet.address,
                  amount: payload.totalAmount,
                  blocksRemaining: blocksRemaining,
                  status: 102,
                  request: payload.request,
                  privateKey: payload.privateKey
                }

                Helpers.log(util.format('[INFO] Worker #%s found %s for [%s] but we need to look for more', cluster.worker.id, payload.totalAmount, payload.wallet.address))

                /* Send the notice to the requestor that we found funds and that we're still looking */
                publicMQ.sendToQueue(Config.queues.complete, waitingForConfirmationsNotEnough, { persistent: true })
                  .then(() => { return privateMQ.nack(message) })
                  .then(() => { return resolve() })
              }
            }
          })
          .then(() => {
            /* If we made it this far, then that means that we probably didn't find any funds */

            /* If we have not found any funds and our window for the request is closed, handle it */
            if (payload.topBlock.height > payload.maxHeight && payload.walletOutputs.length === 0) {
              const response = {
                address: payload.wallet.address,
                keys: {
                  privateSpend: payload.wallet.spend.privateKey,
                  privateView: payload.wallet.view.privateKey
                },
                status: 408, // Request Timed Out
                request: payload.request,
                privateKey: payload.privateKey
              }

              /* Send the 'cancelled' wallet back to the requestor letting them know that we're
                 done processing this request and they can do with it what they will */

              Helpers.log(util.format('[INFO] Worker #%s timed out wallet [%s]', cluster.worker.id, payload.wallet.address))

              publicMQ.sendToQueue(Config.queues.complete, response, { persistent: true })
                .then(() => { return privateMQ.ack(message) })
                .then(() => { return resolve() })
            }

            /* Else, send the request back into the stack */
            return privateMQ.nack(message)
          })
          .then(() => { return resolve() })
          .catch(error => { return reject(error) })
      })
    }

    return run()
      .catch(error => {
        Helpers.log(util.format('[INFO] Error encountered in worker %s: %s', cluster.worker.id, error.toString()))
        privateMQ.nack(message).catch((error) => Helpers.log(util.format('Worker #%s: Could not nack message [%s]', cluster.worker.id, error.toString())))
      })
  })

  publicMQ.connect()
    .then(() => { return publicMQ.createQueue(Config.queues.complete) })
    .then(() => { return privateMQ.connect() })
    .then(() => { return privateMQ.createQueue(Config.queues.send) })
    .then(() => { return privateMQ.registerConsumer(Config.queues.scan, 1) })

  Helpers.log(util.format('Worker #%s awaiting requests', cluster.worker.id))
}
