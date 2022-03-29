'use strict';
const connection = require('./connection').connection;
const preparation = require('./preparation');
const integrityChecks = require('./integrity-checks');
const prepare = preparation.prepare;
const insertSource = preparation.insertSource;
const movePersonsUsersAndAssociatedTables = require('./person-users');
const locationsCopier = require('./location');
const patientsCopier = require('./patient');
const programsCopier = require('./patient-programs');
const providersCopier = require('./provider');
const visitsCopier = require('./visit');
const encounterCopier = require('./encounter');
const personAttributesCopier = require('./person-attribute');
const obsCopier = require('./obs');
const gaacModuleTablesCopier = require('./gaac');
const utils = require('./utils');
const logTime = utils.logTime;
const config = require('./config');


async function orchestration() {
    let persist = config.persist || false;
    let startTime = Date.now();
    let initialErrors = [];
    let dryRun = process.argv.some(arg => (arg === '--dry-run'));

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

        if(dryRun) {
            utils.logInfo(logTime(), ': Partial preparation for DRY run');
            await preparation.prepareForDryRun(srcConn, destConn, config);
        } else {
            utils.logInfo(logTime(), ': Preparing destination database...');
            await prepare(srcConn,destConn, config);
        }

        utils.logInfo(logTime(), ': Checking for Orphaned Records');
        await integrityChecks(srcConn, config.source.openmrsDb);

        utils.logInfo(logTime(), ': Starting data migration ...');
        destConn.query('START TRANSACTION');
        await movePersonsUsersAndAssociatedTables(srcConn, destConn);

        utils.logInfo('Consolidating locations...');
        let movedLocations = await locationsCopier(srcConn, destConn);
        utils.logOk(`Ok...${movedLocations} locations moved.`);

        //patients & identifiers
        await patientsCopier(srcConn, destConn);

        //programs
        await programsCopier(srcConn, destConn);

        //providers & provider attributes
        await providersCopier(srcConn, destConn);

        //visits & visit types
        await visitsCopier(srcConn, destConn);

        //encounters, encounter_types, encounter_roles & encounter_providers
        await encounterCopier(srcConn, destConn);

        // person_attribute_type, person_attribute
        await personAttributesCopier(srcConn, destConn);
        
        //obs
        await obsCopier(srcConn, destConn);

        //gaac tables
        await gaacModuleTablesCopier(srcConn, destConn);

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
        utils.logInfo(`Time elapsed: ${timeElapsed} ms`);
    }
}

module.exports = orchestration; // In case one needs to require it.

// Run
orchestration();
