// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const EventEmitter = require('events')
const RabbitMQ = require('amqplib')
const UUID = require('uuid/v4')

class Rabbit extends EventEmitter {
  constructor (host, username, pass, autoReconnect) {
    autoReconnect = autoReconnect || true
    if (!host) throw new Error('Must supply a host to connect to')
    super()

    this.connectionString = buildConnectionString(host, username, pass)
    this.replyQueue = UUID().toString().replace(/-/g, '')

    if (autoReconnect) {
      this.on('disconnect', (error) => {
        this.emit('log', 'Error: ' + error.toString())
        this.emit('log', 'Reconnecting to server...')
        this.connect().catch((error) => {
          this.emit('log', 'Could not reconnect to server:' + error.toString())
        })
      })
    }
  }

  ack (message) {
    return this.channel.ack(message)
  }

  connect () {
    return RabbitMQ.connect(this.connectionString)
      .then((connection) => {
        connection.on('disconnect', (error) => this.emit('disconnect', error))

        return connection.createChannel()
      })
      .then((channel) => {
        channel.on('disconnect', error => this.emit('disconnect', error))
        this.channel = channel

        return this.createQueue(this.replyQueue, false, true)
      })
      .then(() => { this.emit('connect') })
  }

  createQueue (queue, durable, exclusive) {
    durable = durable || true

    return this.channel.assertQueue(queue, {
      durable: durable,
      exclusive: exclusive || false
    })
  }

  nack (message) {
    return this.channel.nack(message)
  }

  prefetch (value) {
    return this.channel.prefetch(value)
  }

  registerConsumer (queue, prefetch) {
    if (typeof queue !== 'string') throw new Error('Queue name must be a string')

    if (prefetch) {
      this.prefetch(prefetch)
    }

    this.channel.consume(queue, (message) => {
      if (message !== null) {
        const payload = JSON.parse(message.content.toString())

        this.emit('message', queue, message, payload)
      }
    })
  }

  reply (message, payload) {
    return this.sendToQueue(message.properties.replyTo, payload, {
      correlationId: message.properties.correlationId
    })
  }

  requestReply (queue, payload, timeout) {
    if (typeof queue !== 'string') throw new Error('Queue name must be a string')
    timeout = timeout || 5000

    return new Promise((resolve, reject) => {
      const requestId = UUID().toString().replace(/-/g, '')
      var cancelTimer

      this.channel.consume(this.replyQueue, (message) => {
        if (message !== null && message.properties.correlationId === requestId) {
          const response = JSON.parse(message.content.toString())

          this.ack(message)

          if (cancelTimer !== null) {
            clearTimeout(cancelTimer)
          }

          return resolve(response)
        } else {
          this.nack(message)
        }
      })

      this.sendToQueue(queue, payload, {
        correlationId: requestId,
        replyTo: this.replyQueue,
        expiration: timeout
      })

      cancelTimer = setTimeout(() => {
        return reject(new Error('Could not complete request within the specified timeout period'))
      }, timeout + 500)
    })
  }

  sendToQueue (queue, payload, options) {
    if (typeof queue !== 'string') throw new Error('Queue name must be a string')

    if (!(payload instanceof Buffer)) {
      payload = Buffer.from(JSON.stringify(payload))
    }

    this.channel.sendToQueue(queue, payload, options)
  }
}

function buildConnectionString (host, username, pass) {
  const result = ['amqp://']

  if (username.length !== 0 && pass.length !== 0) {
    result.push(username + ':')
    result.push(pass + '@')
  }

  result.push(host)

  return result.join('')
}

module.exports = Rabbit
