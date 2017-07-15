'use strict';

const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');

class Mongodb {

    constructor({ logger, config }) {
        this.log = logger;
        this.conf = config;
    }

    connect() {
        this.loadModels();
        const promise = new Promise(function _mongoExecutor(resolve, reject) {
            mongoose.set('debug', this.conf.debug);
            const connectionString = `${this.conf.connectionString}/${this.conf.database}`;
            mongoose.connect(connectionString,
                this.conf.db,
                this.conf.server,
                this.conf.replset,
                this.conf.user,
                this.conf.pass,
                (err) => {
                    if (err) {
                        this.log.emit('info', `[MONGODB][${err}]`);
                        return reject(err);
                    }
                    this.log.emit('info', `[MONGODB] Connected ${connectionString}`);
                    return resolve();
                }
            );
        });

        return promise;
    }

    loadModels() {
        forDir(this.conf.models || `${process.pwd()}/models`, function (file) {
            try {
                if (path.extname(file) === '.js') {
                    require(file);
                }
            } catch (e) {
                this.log.emit('error', e);
                throw e;
            }
        });
    }
}

function forDir(path, loadTemplates, fn) {
    if (!fn) {
        fn = loadTemplates;
        loadTemplates = false;
    }

    const dir = fs.readdirSync(path);

    dir.forEach(function eachDirFn(file) {
        if (fs.lstatSync(path + '/' + file).isFile()) {
            if (!loadTemplates && file.indexOf('_template') > -1) {
                return;
            }
            return fn(path + '/' + file);
        }
        return forDir(`${path}/${file}/`, loadTemplates, fn);
    });
}

module.exports = Mongodb;
