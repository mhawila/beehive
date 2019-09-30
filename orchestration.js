'use strict';
const connection = require('./connection').connection;
const preparation = require('./preparation');
const uuidChecks = require('./uuid-checks');
const integrityChecks = require('./integrity-checks');
const prepare = preparation.prepare;
const insertSource = preparation.insertSource;
const movePersonsUsersAndAssociatedTables = require('./person-users');
const locationsMover = require('./location');
const patientsMover = require('./patient');
const programsMover = require('./patient-programs');
const providersMover = require('./provider');
const visitsMover = require('./visit');
const encounterMover = require('./encounter');
const obsMover = require('./obs');
const gaacModuleTablesMover = require('./gaac');
const utils = require('./utils');
const logTime = utils.logTime;
const config = require('./config');


async function orchestration(dryRun) {
    let persist = config.persist || false;
    let startTime = Date.now();
    let initialErrors = [];

    if(dryRun === undefined || dryRun === null) {
        if(process.argv && typeof process.argv === 'Array') {
            dryRun = process.argv.some(arg => (arg === '--dry-run'));
        } else {
            dryRun = false;
        }
    }

    if (config.source.location === undefined) {
        initialErrors.push('Error: source.location not specified in config.json file');
    }

    if (config.generateNewUuids === undefined) {
        let msg = 'Error: generateNewUuids option must be explicitly set to true/false ' +
            'in config.json file';
        initialErrors.push(msg);
    }

    if (initialErrors.length > 0) {
        initialErrors.forEach(error => {
            utils.logError(error);
        });
        utils.logInfo('Aborting...');
        process.exit(1);
    }

    let srcConn = null;
    let destConn = null;
    try {
        srcConn = await connection(config.source);
        destConn = await connection(config.destination);

        // Check for UUID collisions

        if(!dryRun) {
            utils.logInfo(logTime(), ': Preparing destination database...');
            await prepare(srcConn,destConn, config);
        }

        utils.logInfo(logTime(), ': Checking for Orphaned Records');
        await integrityChecks(srcConn, config.source.openmrsDb);

        utils.logInfo(logTime(), ': Ensuring uniqueness of UUIDs');
        await uuidChecks(srcConn, destConn, dryRun, true);

        utils.logInfo(logTime(), ': Starting data migration ...');
        destConn.query('START TRANSACTION');
        await movePersonsUsersAndAssociatedTables(srcConn, destConn);

        utils.logInfo('Consolidating locations...');
        let movedLocations = await locationsMover(srcConn, destConn);
        utils.logOk(`Ok...${movedLocations} locations moved.`);

        //patients & identifiers
        await patientsMover(srcConn, destConn);

        //programs
        await programsMover(srcConn, destConn);

        //providers & provider attributes
        await providersMover(srcConn, destConn);

        //visits & visit types
        await visitsMover(srcConn, destConn);

        //encounters, encounter_types, encounter_roles & encounter_providers
        await encounterMover(srcConn, destConn);

        //obs
        await obsMover(srcConn, destConn);

        //gaac tables
        await gaacModuleTablesMover(srcConn, destConn);

        if (!persist) {
            await insertSource(destConn, config.source.location);
        }

        if(dryRun) {
            destConn.query('ROLLBACK');
            utils.logOk(`Done...No database changes have been made!`)
        }
        else {
            destConn.query('COMMIT');
            utils.logOk(`Done...All Data from ${config.source.location} copied.`);
        }
    } catch (ex) {
        destConn.query('ROLLBACK');
        utils.logError(ex);
        utils.logInfo('Aborting...Rolled back, no data has been moved');
    } finally {
        if (srcConn) srcConn.end();
        if (destConn) destConn.end();
        let timeElapsed = (Date.now() - startTime);
        utils.logInfo(`Time elapsed: ${utils.getSimpleTimeString(timeElapsed)}`);

        // Message Queue if not null.
        if(global.progressMessageQueue !== undefined && global.progressMessageQueue !== null) {
            global.progressMessageQueue.emit('end');
        }
    }
}

module.exports = orchestration;
