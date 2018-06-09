require('tungus');
const mongoose = require('mongoose');
const Schema = mongoose.Schema;
const _ = require('lodash');
const autopopulate = require('mongoose-autopopulate');

console.log('Running mongoose version %s', mongoose.version);

const consoleSchema = Schema({
    name: String,
    manufacturer: String,
    released: Date
});
const Console = mongoose.model('Console', consoleSchema);

/**
 * Game schema
 */
const gameSchema = Schema({
    name: String,
    developer: String,
    released: Date,
    console1: {type: Schema.Types.ObjectId, ref: 'Console', autopopulate: true}
});

gameSchema.plugin(autopopulate);

const Game = mongoose.model('Game', gameSchema);

/**
 * Connect to the console database on localhost with
 * the default port (27017)
 */

mongoose.Promise = global.Promise;
mongoose.connect('tingodb://test2', {useMongoClient: false}, function (err) {
    // if we failed to connect, abort
    if (err) throw err;

    // we connected ok
    example();
})

/**
 * Population
 */

async function example() {
    try {
        await Console.remove({});
        const console = await Console.create({name: 'test'});
        await Game.remove({});
        await Game.create({
            name: 'Legend of Zelda: Ocarina of Time',
            developer: 'Nintendo',
            released: new Date('November 21, 1998'),
            console1: console._id
        });

        const count = await Game.count({});

        const game = await Game.findOne({});
        await Game.findByIdAndUpdate(game._id, {$set: {name: 'Test'}});

        const games = await Game.find({console: {$exists: false}}).lean();
        const a = 5;
    } catch (e) {
        console.warn(e);
    }
}