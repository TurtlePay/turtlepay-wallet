// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const util = require('util')

class Helpers {
  static log (message) {
    log(message)
  }
}

function log (message) {
  console.log(util.format('%s: %s', (new Date()).toUTCString(), message))
}

module.exports = Helpers
