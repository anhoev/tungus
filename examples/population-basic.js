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
    test: {
        type: [{
            name: String,
            color: {type: String, form: {type: 'color'}},
            bigButton: Boolean,
            bigButtonVertical: Boolean,
            item: {
                type: [{
                    text: String,
                    mode: {type: String, default: 'ARTIKEL'},
                    console1: {
                        type: mongoose.Schema.Types.ObjectId,
                        ref: 'Console',
                        autopopulate: true,
                        label: 'Speise'
                    },
                    color: {type: String, form: {type: 'color'}}
                }],
            }
        }]
    },
    console1: [{type: Schema.Types.ObjectId, ref: 'Console', autopopulate: true}]
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
            console1: [console._id, console._id],
            test: [{
                name : '1',
                item: [{
                    console1: console
                }]
            }]
        });

        //const count = await Game.count({});

        const game = await Game.findOne({});
        await Game.findByIdAndUpdate(game._id, game.toObject());

        const game1 = await Game.findOne({console: {$exists: false}});
        const a = 5;
    } catch (e) {
        console.warn(e);
    }
}