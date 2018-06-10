const MongooseCollection = require('mongoose/lib/collection');
//const Collection = require('./tingodb').Collection;
const utils = require('mongoose/lib/utils');
const LinvoDB = require("linvodb3");
const Collection = require('linvodb3/lib/model');
const _ = require('lodash');

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

        this.collection = new LinvoDB(this.name, {}, {
            filename: `${this.conn.uri.substr(10)}/${this.name}`
        });
        callback(null, this.collection);
        return this.collection;

    }

    insert(doc, opt, cb) {
        this.collection.insert(doc, cb);
    }

    drop(cb) {
        cb();
    }

    ensureIndex(obj, options, cb) {
        const fieldName = _.map(obj, (v, k) => k)[0];
        try {
            this.collection.ensureIndex({fieldName}, cb);
        } catch (e) {
            console.log(this.name);
            console.warn(e);
        }
    }

    createIndex(obj, options) {
        return new Promise((resolve, reject) => {
            const fieldName = _.map(obj, (v, k) => k)[0];
            try {
                this.collection.ensureIndex({fieldName}, () => {
                    resolve();
                });
            } catch (e) {
                console.log(this.name);
                console.warn(e);
            }
        })
    }

    find(query, fields, _cb) {
        const cb = function (err, docs) {
            _cb(err, {
                toArray: cb2 => {
                    cb2(null, docs);
                }
            })
        }

        this.collection.find(query, cb);
    }

    findAndModify(query, sort, update, opts, cb) {
        normalize(update.$set);
        if (update.$set._id) delete update.$set._id;
        if (update.$setOnInsert) delete update.$setOnInsert;
        const _cb = (err, res) => {
            if (err) {
                return cb(err)
            }
            cb(null, {value: res, ok: 1});

        }
        this.collection.update(query, update, opts, _cb);
    }

    remove(query, opts, cb) {
        this.collection.remove(query, Object.assign(opts, {multi: true}), cb);
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

}

function iter(i) {
    TingoCollection.prototype[i] = function (...args) {
        // If user force closed, queueing will hang forever. See #5664
        if (this.opts.$wasForceClosed) {
            return this.conn.db.collection(this.name)[i].apply(this.collection, args);
        }
        if (this.buffer) {
            this.addQueue(i, arguments);
            return;
        }

        let debug = this.conn.base.options.debug;

        if (debug) {
            if (typeof debug === 'function') {
                debug.apply(this, [this.name, i].concat(utils.args(args, 0, args.length - 1)));
            } else {
                this.$print(this.name, i, args);
            }
        }

        try {
            return this.collection[i](args[0], args[args.length - 1]);
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
        if (['insert', 'find', 'remove', 'findAndModify', 'ensureIndex', 'createIndex'].includes(i)) continue;
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

const normalize = function (obj) {
    var self = this;
    _.each(obj, function (v, k) {
        if (_.isObject(v)) {
            if (v.isMongooseArray) {
                obj[k] = obj[k].toObject();
            } else {
                normalize(v);
            }
        }
    });
    return obj;
};

/*!
 * Module exports.
 */

module.exports = TingoCollection;
