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
const Rabbit = require('./lib/rabbit')
const request = require('request-promise-native')
const TurtleCoinUtils = require('turtlecoin-utils').CryptoNote
const util = require('util')

const cpuCount = require('os').cpus().length
const cryptoUtils = new TurtleCoinUtils()

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
      /* If the payload is encrypted, we need to decrypt it */
      if (payload.encrypted) {
        payload = crypto.decrypt(payload.encrypted)
      }

      /* Sort our outputs from smallest to largest */
      payload.funds.sort((a, b) => (a.amount > b.amount) ? 1 : ((b.amount > a.amount) ? -1 : 0))

      /* Pull some of the information we need for further processing
           out of the outputs we received */
      payload.totalToSend = 0
      const amounts = []
      for (var i = 0; i < payload.funds.length; i++) {
        payload.totalToSend += payload.funds[i].amount
        amounts.push(payload.funds[i].amount)
      }

      /* Save off the amount that we actually received */
      payload.amountReceived = payload.totalToSend

      /* If this is a view only wallet, we aren't actually going to
         send anything on, because we don't have access to the wallet
         private spend key to do so */
      if (payload.viewOnly) {
        const response = {
          address: payload.wallet.address,
          paymentId: payload.wallet.paymentId,
          amountReceived: payload.amountReceived,
          transactions: payload.txs,
          status: 200, // OK
          request: payload.request,
          privateKey: payload.privateKey
        }

        return publicMQ.sendToQueue(Config.queues.complete, response, { persistent: true })
          .then(() => {
            payload.tx = { hash: payload.txs.join(',') }
            Helpers.log(util.format('[INFO] Worker #%s skipping delivery of [%s] from [%s] in transaction(s) [%s] as it is view only', cluster.worker.id, payload.amountReceived, payload.wallet.address, payload.tx.hash))
          })
      }

      return new Promise((resolve, reject) => {
        const transactionFee = payload.fee || Config.defaultNetworkFee
        /* If there is less funds available than the network fee
           we can't really do anything. We're not going to pay to
           to send on funds we didn't receive enough for to at least
           cover our costs. */
        if (payload.totalToSend < transactionFee) {
          /* Generate the response for sending it back to the requestor,
             to let them know that funds were received (still incomplete) */
          var goodResponse = {
            address: payload.wallet.address,
            status: 402, // Payment required (as in not enough to send anything on
            request: payload.request,
            privateKey: payload.privateKey
          }

          publicMQ.sendToQueue(Config.queues.complete, goodResponse, { persistent: true })

          /* We are going to close this request out because we're
             not going to do anything else with this */
          Helpers.log(util.format('[INFO] Worker #%s encountered wallet with insufficient funds [%s]', cluster.worker.id, payload.wallet.address))
          return resolve()
        }

        /* Deduct the network transaction fee */
        payload.totalToSend -= transactionFee

        /* If there's still funds to send, we need to create the necessary
           outputs to send them the funds. The way that createOutputs
           is implemented in the utils library, every transaction we create
           is ~almost~ a fusion transaction */
        if (payload.totalToSend !== 0) {
          payload.outputs = cryptoUtils.createTransactionOutputs(payload.request.address, payload.totalToSend)
        }

        /* Let's go get our random outputs to mix with */
        request({
          url: Config.randomOutputsUrl,
          json: true,
          method: 'POST',
          body: {
            mixin: Config.defaultMixinCount,
            amounts: amounts
          }
        })
          .then(randomOutputs => {
            /* Validate that we received enough randomOutputs to complete the request */
            if (randomOutputs.length !== payload.funds.length) {
              /* Something didn't work right, let's leave this for someone else to handle */
              return reject(new Error(util.format('[INFO] Worker #%s encountered a request with an invalid number of random outputs [%s]', cluster.worker.id, payload.wallet.address)))
            }

            /* Although the block API returns the random outputs sorted by amount,
               we're going to sort it ourselves quick just to make sure */
            randomOutputs.sort((a, b) => (a.amount > b.amount) ? 1 : ((b.amount > a.amount) ? -1 : 0))

            /* We received more random outputs than we actually need for each of the amounts
               just in case one of the outputs we received matches one we're trying
               to mix with. We need to make some sense of this before trying
               to build our transaction otherwise things are going to get very ugly. */

            /* We'll start looping through our outputs to run sanity checks */
            for (var j = 0; j < payload.funds.length; j++) {
              var saneRandoms = []
              var randoms = randomOutputs[j]

              /* If, for some reason the amounts of the random outputs
                 aren't for this set of funds, something went wrong...
                 very very wrong */
              if (randoms.amount !== payload.funds[j].amount) {
                return reject(new Error('Error handling random outputs'))
              }

              /* Loop through the fake inputs to do some basic checking including
                 that we actually need additional fake inputs */
              randoms.outs.forEach((elem) => {
                if (elem.out_key !== payload.funds[j].keyImage && saneRandoms.length < Config.defaultMixinCount) {
                  /* Toss them in the stack in a way that the library we use
                     requires them */
                  saneRandoms.push({
                    globalIndex: elem.global_amount_index,
                    key: elem.out_key
                  })
                }
              })
              randomOutputs[j] = saneRandoms
            }

            /* Time to clean up our room */
            return new Promise(resolve => { return resolve(randomOutputs) })
          })
          .then(randomOutputs => {
            /* Decode the payload address so that we can figure out if we need to send
               the funds with a payment ID */
            const decodedAddress = cryptoUtils.decodeAddress(payload.request.address)
            var paymentId = null
            if (decodedAddress.paymentId.length !== 0) {
              paymentId = decodedAddress.paymentId
            }

            /* Create the new transaction that will send the funds on */
            payload.tx = cryptoUtils.createTransaction(payload.outputs, payload.funds, randomOutputs, Config.defaultMixinCount, transactionFee, paymentId)

            /* Send the transaction to the relay workers and give it 5s to process */
            return publicMQ.requestReply(Config.queues.relayAgent, payload.tx, 5000)
          })
          .then(workerResponse => {
            if (workerResponse.status && workerResponse.status.toUpperCase() === 'OK') {
              const response = {
                address: payload.wallet.address,
                keys: {
                  privateSpend: payload.wallet.spend.privateKey,
                  privateView: payload.wallet.view.privateKey
                },
                amountReceived: payload.amountReceived,
                amountSent: payload.totalToSend,
                networkFee: transactionFee,
                transactionHash: payload.tx.hash,
                transactionPrivateKey: payload.tx.transaction.prvkey,
                status: 200, // OK
                request: payload.request,
                privateKey: payload.privateKey
              }

              /* Send the 'completed' request information back
                 to the public processors so we can let the requestor
                 know that the process is complete */
              return publicMQ.sendToQueue(Config.queues.complete, response, { persistent: true })
            } else {
              /* For some reason, the relay failed, we'll send it back
                 to the queue to retry later */
              return reject(new Error(util.format('[INFO] Worker #%s failed to relay [%s] from [%s] to [%s]', cluster.worker.id, payload.totalToSend, payload.wallet.address, payload.request.address)))
            }
          })
          .then(() => Helpers.log(util.format('[INFO] Worker #%s relayed [%s] from [%s] to [%s] in transaction [%s]', cluster.worker.id, payload.totalToSend, payload.wallet.address, payload.request.address, payload.tx.hash)))
          .then(() => { return resolve() })
          .catch((error) => { return reject(error) })
      })
    }

    return run()
      .then(() => { return privateMQ.ack(message) })
      .catch((error) => {
        Helpers.log(util.format('[INFO] Error encountered in worker %s: %s', cluster.worker.id, error.toString()))
        privateMQ.nack(message).catch((error) => Helpers.log(util.format('Worker #%s: Could not nack message [%s]', cluster.worker.id, error.toString())))
      })
  })

  publicMQ.connect().then(() => { return publicMQ.createQueue(Config.queues.complete) })
    .then(() => { return publicMQ.createQueue(Config.queues.relayAgent) })
    .then(() => { return privateMQ.connect() })
    .then(() => { return privateMQ.createQueue(Config.queues.send) })
    .then(() => { return privateMQ.registerConsumer(Config.queues.send, 1) })

  Helpers.log(util.format('Worker #%s awaiting requests', cluster.worker.id))
}
