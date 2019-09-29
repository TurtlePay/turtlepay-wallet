// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const crypto = require('crypto')
const AESJS = require('aes-js')

class AES {
  constructor (opts) {
    opts = opts || {}
    if (!opts.password) throw new Error('must supply a password')
    this.key = Buffer.from(crypto.createHmac('sha256', opts.password).digest('hex'), 'hex')
  }

  decrypt (data, json) {
    json = json || true
    const encryptedBytes = AESJS.utils.hex.toBytes(data)
    const AesConstructor = AESJS.ModeOfOperation.ctr
    const aesCtr = new AesConstructor(this.key)
    const decryptedBytes = aesCtr.decrypt(encryptedBytes)
    const decryptedText = AESJS.utils.utf8.fromBytes(decryptedBytes)
    if (json) {
      return JSON.parse(decryptedText)
    } else {
      return decryptedText
    }
  }

  encrypt (data, json) {
    json = json || true
    if (json) {
      data = JSON.stringify(data)
    }
    const dataBytes = AESJS.utils.utf8.toBytes(data)
    const AesConstructor = AESJS.ModeOfOperation.ctr
    const aesCtr = new AesConstructor(this.key)
    const encryptedBytes = aesCtr.encrypt(dataBytes)
    return AESJS.utils.hex.fromBytes(encryptedBytes)
  }
}

module.exports = AES
