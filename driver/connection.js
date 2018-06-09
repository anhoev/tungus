const MongooseConnection = require('mongoose/lib/connection');

const STATES = require('mongoose/lib/connectionstate');

const path = require('path');
const mkdirp = require('mkdirp');

function TingoConnection() {
    MongooseConnection.apply(this, arguments);
}

TingoConnection.prototype.__proto__ = MongooseConnection.prototype;

TingoConnection.STATES = STATES;

TingoConnection.prototype._openWithoutPromise =
    TingoConnection.prototype.openUri =
        TingoConnection.prototype.open = function (uri, options, callback) {
            return new Promise((resolve, reject) => {
                this.uri = uri;
                const handleFunc = (err) => {
                    if (err) {
                        if (!callback) {
                            // Error can be on same tick re: christkv/mongodb-core#157
                            setImmediate(() => this.emit('error', err));
                        }
                        reject(err);
                    }
                    else {
                        resolve();
                        this.onOpen(callback);
                    }
                    callback && callback(err);
                };

                // TODO: Check if path valid on this file system
                if (!uri.startsWith('mongodb://') && !uri.startsWith('tingodb://')) {
                    return handleFunc(`Uri "${uri}" is not valid!`);
                }

                let dbPath = uri.substr(10);
                mkdirp(dbPath, (err, made) => {
                    if (!err) {
                        this.db = {}
                    }
                    handleFunc(err);
                });
            });
        }

TingoConnection.prototype._openSetWithoutPromise =
    TingoConnection.prototype.openSet = function () {
        throw new Error('The TingoDB does not support replication.');
    }


TingoConnection.prototype.doClose = function (force, fn) {
    //this.db.close(force, fn);
    return this;
}

TingoConnection.prototype.parseOptions = function () {
    // TODO: Implement me, some options might be supported by TingoDB...
    // console.info('connection.parseOptions() is not implemented yet', arguments);
    return {};
}

module.exports = TingoConnection;
