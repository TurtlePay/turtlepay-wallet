// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const AES = require('aes-js')
const crypto = require('crypto')

class AESHelper {
  constructor (opts) {
    opts = opts || {}
    if (!opts.password) throw new Error('Must supply a password for encryption')

    this.key = Buffer.from(crypto.createHmac('sha256', opts.password).digest('hex'), 'hex')
  }

  encrypt (data) {
    if (typeof data !== 'string') data = JSON.stringify(data)

    const dataBytes = AES.utils.utf8.toBytes(data)
    const AesConstructor = AES.ModeOfOperation.ctr
    const aesCtr = new AesConstructor(this.key)
    const encryptedBytes = aesCtr.encrypt(dataBytes)
    return AES.utils.hex.fromBytes(encryptedBytes)
  }

  decrypt (data) {
    try {
      const encryptedBytes = AES.utils.hex.toBytes(data)
      const AesConstructor = AES.ModeOfOperation.ctr
      const aesCtr = new AesConstructor(this.key)
      const decryptedBytes = aesCtr.decrypt(encryptedBytes)
      const decryptedText = AES.utils.utf8.fromBytes(decryptedBytes)

      try {
        return JSON.parse(decryptedText)
      } catch (e) {
        return null
      }
    } catch (e) {
      return null
    }
  }
}

module.exports = AESHelper
