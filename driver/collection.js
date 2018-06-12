const MongooseCollection = require('mongoose/lib/collection');
const _ = require('lodash');
const ObjectId = require('bson').ObjectId;
const sift = require('sift');

const levelup = require('levelup');
const leveldown = require('leveldown');
const {compileSort, compileDocumentSelector} = require('minimongo/lib/selector');
const document = require('linvodb3/lib/document')
const multilevel = require('multilevel');
const net = require('net');

class TingoCollection extends MongooseCollection {
    constructor() {
        super(...arguments);
        this.queue2 = [];
        if (this.conn.uri) this.init();
    }

    init() {
        const base = `${this.conn.uri.split('//')[1]}/${this.name}`;
        const createSockPath = dir => {
            return process.platform === 'win32' ?
                '\\\\.\\pipe\\level-party\\' + path.resolve(dir) :
                path.join(dir, 'level-party.sock');
        }

        const _init = () => {
            this.dataDb = levelup(leveldown(`${base}`), {compression: false});
            this.indexDb = levelup(leveldown(`${base}_index`), {compression: false});
        }

        if (this.conn.uri.split('//')[0].includes('server')) {
            net.createServer(con => con.pipe(multilevel.server(this.indexDb)).pipe(con)).listen(createSockPath(base));
            net.createServer(con => con.pipe(multilevel.server(this.dataDb)).pipe(con)).listen(createSockPath(`${base}_index`));
            _init();
        } else if (this.conn.uri.split('//')[0].includes('client')) {
            this.indexDb = multilevel.client();
            const con = net.connect(createSockPath(`${base}_index`));
            con.pipe(db.createRpcStream()).pipe(con);

            this.dataDb = multilevel.client();
            const con2 = net.connect(createSockPath(base));
            con2.pipe(db.createRpcStream()).pipe(con2);
        } else {
            _init();
        }

        this.idx = [];
        this.indexes = [];
        this.indexDb.createReadStream()
            .on('data', data => {
                this.idx.push(document.deserialize(data.value));
            })
            .on('end', () => {
                super.onOpen();
                this.loaded = true;
                this.queue2.forEach(fn => fn());
            })
    }

    onOpen() {
        if (!this.dataDb) this.init();
    }

    getIndex(doc) {
        return _.pick(doc, this.indexes.concat(['_id']));
    }

    insert(doc, opt, cb) {
        normalize(doc);
        this.idx.push(doc);
        this.indexDb.put(doc._id, document.serialize(this.getIndex(doc))).then();
        this.dataDb.put(doc._id, document.serialize(doc), cb);
    }

    drop(cb) {
        cb();
    }

    ensureIndex(obj, options, cb) {
        const fieldNames = _.map(obj, (v, k) => k);
        this.indexes.push(...fieldNames);
    }

    createIndex(obj, options) {
        const fieldNames = _.map(obj, (v, k) => k);
        this.indexes.push(...fieldNames);
    }

    findOne(query, opts, _cb) {
        const _findOne = () => {
            if (opts) delete opts.fields;
            normalize(query);
            let [key] = processFind(this.idx, query, opts).map(doc => doc._id);
            if (key) {
                this.dataDb.get(key, (err, doc) => {
                    _cb(err, document.deserialize(doc));
                });
            } else {
                _cb(null, null);
            }
        }

        if (this.loaded) {
            _findOne();
        } else {
            this.queue2.push(_findOne);
        }
    }

    findAsync(query, opts) {
        return new Promise((resolve, reject) => {
            this.find(query, opts, (err, cursor) => {
                cursor.toArray((err, docs) => {
                    resolve(docs);
                })
            })
        })
    }

    find(query, opts, _cb) {
        const _find = () => {
            if (opts) delete opts.fields;
            normalize(query);
            let keys = processFind(this.idx, query, opts).map(doc => doc._id);
            //let docs = [];
            const cb = function (err, docs) {
                _cb(err, {
                    toArray: cb2 => {
                        cb2(null, docs);
                    }
                })
            }

            if (!_.isEmpty(keys)) {
                Promise.all(keys.map(_id => this.dataDb.get(_id)))
                    .then(docs => {
                        docs = docs.map(doc => document.deserialize(doc));
                        cb(null, docs)
                    })
                    .catch(err => cb(err))
            } else {
                cb(null, []);
            }
        }

        if (this.loaded) {
            _find();
        } else {
            this.queue2.push(_find);
        }
    }

    findAndModify(query, sort, update, opts = {}, cb) {
        normalize(update);
        if (update.$set._id) delete update.$set._id;
        if (update.$setOnInsert) delete update.$setOnInsert;

        this.find(query, opts, (err, _docs) => {
            _docs.toArray((err, docs) => {
                const batch = this.dataDb.batch();
                const batchIndex = this.indexDb.batch();
                for (const doc of docs) {
                    let doc2 = _.assign(doc, update.$set);
                    batch.put(doc._id, document.serialize(doc2));
                    this.idx[_.findKey(this.idx, id => id._id === doc._id)] = this.getIndex(doc2);
                    batchIndex.put(doc._id, document.serialize(this.getIndex(doc2)));
                }
                batchIndex.write(() => {
                    batch.write(() => {
                        cb(null, {value: docs, ok: 1})
                    });
                });
            });
        })
    }

    update(query, update, opts = {}, cb) {
        this.findAndModify(query, {}, update, null, cb);
    }

    remove(query, opts, cb) {
        normalize(query);
        let keys = processFind(this.idx, query, opts).map(doc => doc._id);

        _.remove(this.idx, i => keys.includes(i._id));

        if (keys.length > 0) {
            const batchIndex = this.indexDb.batch();
            const batch = this.dataDb.batch();
            for (const key of keys) {
                if (key) batchIndex.del(key);
                if (key) batch.del(key);
            }

            batchIndex.write(() => {
                batch.write(() => {
                    cb(null, keys.length);
                });
            });

        } else {
            cb(null, 0);
        }
    }

    onClose(force) {
        super.onClose(force);
    }

}

function processFind(items, query, opts) {
    let filtered = sift(query, items);
    if (opts && opts.sort) filtered.sort(compileSort(opts.sort))
    if (opts && opts.skip) filtered = _.slice(filtered, opts.skip)
    if (opts && opts.limit) filtered = _.take(filtered, opts.limit)
    return filtered;
}

const normalize = function (obj) {
    _.each(obj, function (v, k) {
        if (v instanceof ObjectId) {
            obj[k] = v.toString();
        } else if (_.isObject(v)) {
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