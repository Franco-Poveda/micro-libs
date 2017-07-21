'use strict';

const EventEmitter = require('events');

class Logger extends EventEmitter {
    init(opts) {
        const { env, serviceName } = opts;
        const levels = ['info', 'warn', 'error'];

        if (env === 'development') {
            this.on('debug', msg => {
                this.outputLog({
                    logsrc: serviceName,
                    env: env,
                    msg,
                    level: 'debug'
                });
            });
        }

        levels.forEach(level => {
            this.on(level, msg => {
                this.outputLog({
                    logsrc: serviceName,
                    env: env,
                    msg,
                    level: level
                });
            });
        });
    }

    outputLog(data) {
        console.log(data.msg);
    }
}

module.exports = new Logger;
