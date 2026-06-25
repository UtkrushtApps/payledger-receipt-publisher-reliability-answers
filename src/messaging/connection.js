const amqp = require('amqplib');
const config = require('../config');
const { createLogger } = require('../utils/logger');

const log = createLogger('connection');

let connection = null;
let connectionPromise = null;
let channel = null;
let channelPromise = null;
let confirmChannel = null;
let confirmChannelPromise = null;

function resetConnectionState(conn) {
  if (!conn || conn === connection) {
    connection = null;
    channel = null;
    confirmChannel = null;
  }
}

async function getOrCreateConnection() {
  if (connection) {
    return connection;
  }

  if (connectionPromise) {
    return connectionPromise;
  }

  connectionPromise = amqp.connect(config.amqpUrl)
    .then((conn) => {
      connection = conn;

      conn.on('error', (err) => {
        log.error({ err: err.message }, 'amqp connection error');
      });

      conn.on('close', () => {
        log.warn('amqp connection closed');
        resetConnectionState(conn);
      });

      log.info('amqp connection established');
      return conn;
    })
    .finally(() => {
      connectionPromise = null;
    });

  return connectionPromise;
}

function attachChannelHandlers(ch, kind) {
  ch.on('error', (err) => {
    log.error({ err: err.message, kind }, 'amqp channel error');
  });

  ch.on('close', () => {
    log.warn({ kind }, 'amqp channel closed');
    if (kind === 'regular' && channel === ch) {
      channel = null;
    }
    if (kind === 'confirm' && confirmChannel === ch) {
      confirmChannel = null;
    }
  });
}

async function connect() {
  if (channel) {
    return channel;
  }

  if (channelPromise) {
    return channelPromise;
  }

  channelPromise = getOrCreateConnection()
    .then((conn) => conn.createChannel())
    .then((ch) => {
      channel = ch;
      attachChannelHandlers(ch, 'regular');
      log.info('amqp regular channel established');
      return ch;
    })
    .finally(() => {
      channelPromise = null;
    });

  return channelPromise;
}

async function connectConfirm() {
  if (confirmChannel) {
    return confirmChannel;
  }

  if (confirmChannelPromise) {
    return confirmChannelPromise;
  }

  confirmChannelPromise = getOrCreateConnection()
    .then((conn) => conn.createConfirmChannel())
    .then((ch) => {
      confirmChannel = ch;
      attachChannelHandlers(ch, 'confirm');
      log.info('amqp confirm channel established');
      return ch;
    })
    .finally(() => {
      confirmChannelPromise = null;
    });

  return confirmChannelPromise;
}

function getChannel() {
  return channel;
}

function getConfirmChannel() {
  return confirmChannel;
}

function getConnection() {
  return connection;
}

async function invalidateConfirmChannel() {
  const ch = confirmChannel;
  confirmChannel = null;
  if (ch) {
    try {
      await ch.close();
    } catch (err) {
      log.warn({ err: err.message }, 'error while invalidating confirm channel');
    }
  }
}

async function close() {
  try {
    if (confirmChannel) {
      await confirmChannel.close();
    }
    if (channel) {
      await channel.close();
    }
    if (connection) {
      await connection.close();
    }
  } catch (err) {
    log.warn({ err: err.message }, 'error while closing amqp resources');
  } finally {
    confirmChannel = null;
    channel = null;
    connection = null;
  }
}

module.exports = {
  connect,
  connectConfirm,
  getChannel,
  getConfirmChannel,
  getConnection,
  invalidateConfirmChannel,
  close,
};
