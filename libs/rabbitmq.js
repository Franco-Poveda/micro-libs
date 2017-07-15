'use strict';

const amqp = require('amqplib');

class Rabbitmq {

    constructor({ logger, config }) {
        this.log = logger;
        this.conf = config;
    }

    connect() {
        return amqp.connect(this.conf.uri).then(conn => {
            conn.on('error', err => {
                this.log.emit('error', `[AMQP]: [${err.message}]`);
            });
            conn.on('close', () => {
                this.log.emit('info', '[AMQP]: connection closed');
            });
            this.log.emit('info', '[AMQP]: connected');
            this.amqpConn = conn;
            return conn;
        }).catch(err => {
            this.log.emit('error', `[AMQP]: [${err.message}]`);
            throw err;
        });
    }

    _openOutChannel() {
        if (!this.conf.out) {
            return false;
        }
        const outCh = this.conf.out;
        return this.amqpConn.createConfirmChannel()
            .then(ch => ch.assertExchange(
                outCh.exchange.name,
                outCh.exchange.type,
                outCh.exchange.options)
                .then(() => ch.bindQueue(
                    outCh.queue.name,
                    outCh.exchange.name,
                    outCh.queue.binding))
                .then(() => ch));
    }

    _openInChannel() {
        if (!this.conf.in) {
            return false;
        }
        const inCh = this.conf.in;
        return this.amqpConn.createChannel()
            .then(ch => ch.assertQueue(
                inCh.queue.name,
                inCh.queue.options)
                .then(() => ch.bindQueue(
                    inCh.queue.name,
                    inCh.exchange.name,
                    inCh.queue.binding))
                .then(() =>  ch));
    }

    openChannels() {
        return Promise.all([
            this._openInChannel(),
            this._openOutChannel()
        ]);
    }
}

module.exports = Rabbitmq;
