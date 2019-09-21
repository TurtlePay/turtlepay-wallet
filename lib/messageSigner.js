// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const crypto = require('crypto')
const pem = require('pem')

function Self (opts) {
  opts = opts || {}
  if (!(this instanceof Self)) return new Self(opts)
  this.algo = opts.algo || 'sha256'
  this.keySize = opts.size || 2048
}

Self.prototype.sign = function (message, privateKey) {
  return new Promise((resolve, reject) => {
    try {
      /* Shadow the message into a SHA256 digest */
      message = this.digest(message)

      /* Shadow the private key into a buffer */
      privateKey = Buffer.from(privateKey, 'hex').toString()

      /* Create the signer object add feed the
         message digest to that signer */
      const signer = crypto.createSign(this.algo)
      signer.update(message)
      signer.end()

      /* Pull the signature out of the signer as hexadecimal */
      const signature = signer.sign(privateKey).toString('hex')

      /* Spit it all back */
      return resolve({ digest: message, signature: signature })
    } catch (error) {
      return reject(error)
    }
  })
}

Self.prototype.digest = function (message, encoding) {
  /* If we weren't supplied with a result encoding
     we will default to hex. See Buffer documentation
     for other available options */
  encoding = encoding || 'hex'

  /* Shadow the message into a JSON message so that
     we can flip arbitrary object types in to this
     method */
  message = JSON.stringify(message)

  /* Create and return the digest of the message */
  return crypto.createHash(this.algo).update(message).digest(encoding)
}

Self.prototype.verify = function (message, publicKey, signature) {
  return new Promise((resolve, reject) => {
    try {
      /* Shadow the message into a SHA256 digest */
      message = this.digest(message)

      /* Shadow the public key into a buffer */
      publicKey = Buffer.from(publicKey, 'hex').toString()

      /* Shadow the signature into a buffer */
      signature = Buffer.from(signature, 'hex')

      /* Create the verifier object feed the message
         digest to that verifier */
      const verifier = crypto.createVerify(this.algo)
      verifier.update(message)
      verifier.end()

      /* Pull the verification status out of the verifier */
      const verified = verifier.verify(publicKey, signature)

      /* Return the result to the caller */
      return resolve(verified)
    } catch (error) {
      return reject(error)
    }
  })
}

Self.prototype.generateKeys = function () {
  return new Promise((resolve, reject) => {
    /* Create a private key of keySize length */
    pem.createPrivateKey(this.keySize, {}, (error, privateKey) => {
      if (error) return reject(error)

      /* Create the matching public key */
      pem.getPublicKey(privateKey.key, (error, publicKey) => {
        if (error) return reject(error)
        try {
          /* Return the key pair to the caller */
          return resolve({ privateKey: Buffer.from(privateKey.key).toString('hex'), publicKey: Buffer.from(publicKey.publicKey).toString('hex') })
        } catch (error) {
          return reject(error)
        }
      })
    })
  })
}

module.exports = Self
