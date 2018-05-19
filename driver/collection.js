/*!
 * Module dependencies.
 */

const MongooseCollection = require('mongoose/lib/collection');
const Collection = require('./tingodb').Collection;
const utils = require('mongoose/lib/utils');

/**
 * A [TingoDB](https://github.com/sergeyksv/tingodb) collection implementation.
 *
 * All methods methods from the [TingoDB](https://github.com/sergeyksv/tingodb) collection are copied and wrapped in queue management.
 *
 * @inherits Collection
 * @api private
 */

class TingoCollection extends MongooseCollection {
    constructor() {
        super(...arguments);
        this.collection = null;
    }

    onOpen() {
        // always get a new collection in case the user changed host:port
        // of parent db instance when re-opening the connection.

        const callback = (err, collection) => {
            if (err) {
                // likely a strict mode error
                this.conn.emit('error', err);
            } else {
                this.collection = collection;
                super.onOpen();
            }
        };

        if (!this.opts.capped.size) {
            // non-capped
            callback(null, this.conn.db.collection(this.name));
            return this.collection;
        }

        // capped
        return this.conn.db.collection(this.name, (err, c) => {
            if (err) return callback(err);

            // discover if this collection exists and if it is capped
            this.conn.db.listCollections({name: this.name}).toArray((err, docs) => {
                if (err) {
                    return callback(err);
                }
                const doc = docs[0];
                const exists = !!doc;

                if (exists) {
                    if (doc.options && doc.options.capped) {
                        callback(null, c);
                    } else {
                        callback(new Error(`A non-capped collection exists with the name: ${this.name}
                                 To use this collection as a capped collection, please first convert it.
                                 http://www.mongodb.org/display/DOCS/Capped+Collections#CappedCollections-Convertingacollectiontocapped`));
                    }
                } else {
                    // create
                    const opts = utils.clone(this.opts.capped);
                    opts.capped = true;
                    this.conn.db.createCollection(this.name, opts, callback);
                }
            });
        });
    }

    onClose(force) {
        super.onClose(force);
    }

    $print(name, i, args) {
        const moduleName = '\x1B[0;36mMongoose:\x1B[0m ';
        const functionCall = [name, i].join('.');
        const _args = [];
        for (let j = args.length - 1; j >= 0; --j) {
            if (this.$format(args[j]) || _args.length) _args.unshift(this.$format(args[j]));
        }
        const params = `(${_args.join(', ')})`;
        console.error(moduleName + functionCall + params);
    }

    $format(arg) {
        const type = typeof arg;
        if (type === 'function' || type === 'undefined') return '';
        return format(arg);
    }

    findAndModify() {
        return this._findAndModify200(...arguments);
    }

    getIndexes() {
        throw new Error('TingoDB does not support indexInformation()');
    }

    mapReduce() {
        this.collection.mapReduce(...arguments);
    }

    aggregate() {
        throw new Error('TingoDB does not support aggregate');
    }
}

function iter(i) {
    TingoCollection.prototype[i] = function () {
        // If user force closed, queueing will hang forever. See #5664
        if (this.opts.$wasForceClosed) {
            return this.conn.db.collection(this.name)[i].apply(collection, args);
        }
        if (this.buffer) {
            this.addQueue(i, arguments);
            return;
        }

        var collection = this.collection;
        var args = arguments;
        var _this = this;
        var debug = _this.conn.base.options.debug;

        if (debug) {
            if (typeof debug === 'function') {
                debug.apply(_this,
                    [_this.name, i].concat(utils.args(args, 0, args.length - 1)));
            } else {
                this.$print(_this.name, i, args);
            }
        }

        try {
            return collection[i].apply(collection, args);
        } catch (error) {
            // Collection operation may throw because of max bson size, catch it here
            // See gh-3906
            if (args.length > 0 &&
                typeof args[args.length - 1] === 'function') {
                args[args.length - 1](error);
            } else {
                throw error;
            }
        }
    };
}

for (let i in Collection.prototype) {
    // Janky hack to work around gh-3005 until we can get rid of the mongoose
    // collection abstraction
    try {
        if (typeof Collection.prototype[i] !== 'function') {
            continue;
        }
        if (i === 'mapReduce') continue;
    } catch (e) {
        continue;
    }

    iter(i);
}

function map(o) {
    return format(o, true);
}

function formatObjectId(x, key) {
    const representation = `ObjectId("${x[key].toHexString()}")`;
    x[key] = {inspect: () => representation};
}

function formatDate(x, key) {
    const representation = `new Date("${x[key].toUTCString()}")`;
    x[key] = {inspect: () => representation};
}

function format(obj, sub) {
    if (obj && typeof obj.toBSON === 'function') {
        obj = obj.toBSON();
    }
    let x = utils.clone(obj, {retainKeyOrder: 1, transform: false});
    let representation;

    if (x !== null) {
        if (x.constructor.name === 'Binary') {
            x = `BinData(${x.sub_type}, "${x.toString('base64')}")`;
        } else if (x.constructor.name === 'ObjectID') {
            representation = `ObjectId("${x.toHexString()}")`;
            x = {
                inspect: function () {
                    return representation;
                }
            };
        } else if (x.constructor.name === 'Date') {
            representation = `new Date("${x.toUTCString()}")`;
            x = {inspect: () => representation};
        } else if (x.constructor.name === 'Object') {
            const keys = Object.keys(x);
            const numKeys = keys.length;
            let key;
            for (let i = 0; i < numKeys; ++i) {
                key = keys[i];
                if (x[key]) {
                    if (typeof x[key].toBSON === 'function') {
                        x[key] = x[key].toBSON();
                    }
                    if (x[key].constructor.name === 'Binary') {
                        x[key] = `BinData(${x[key].sub_type}, "${x[key].buffer.toString('base64')}")`;
                    } else if (x[key].constructor.name === 'Object') {
                        x[key] = format(x[key], true);
                    } else if (x[key].constructor.name === 'ObjectID') {
                        formatObjectId(x, key);
                    } else if (x[key].constructor.name === 'Date') {
                        formatDate(x, key);
                    } else if (Array.isArray(x[key])) {
                        x[key] = x[key].map(map);
                    }
                }
            }
        }
        if (sub) return x;
    }

    return require('util')
        .inspect(x, false, 10, true)
        .replace(/\n/g, '')
        .replace(/\s{2,}/g, ' ');
}

/*!
 * Module exports.
 */

module.exports = TingoCollection;
