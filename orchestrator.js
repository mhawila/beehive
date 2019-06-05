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
const beehive = global.beehive;


async function orchestration() {
    let startTime = Date.now();
    let initialErrors = [];
    let dryRun = global.dryRun = process.argv.some(arg => (arg === '--dry-run'));

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

        await destConn.query('SET autocommit = 0');
        await destConn.query('START TRANSACTION');
        utils.logInfo(logTime(), ': Preparing destination database...');
        await prepare(srcConn,destConn, config);

        if(dryRun || (global.startingStep['atomic_step'] === 'pre-obs'
                                                        && global.startingStep['passed'] == 0)) {
            utils.logInfo(logTime(), ': Performing step: pre-obs');
            utils.logInfo(logTime(), ': Checking for Orphaned Records');
            await integrityChecks(srcConn, config.source.openmrsDb);

            // utils.logInfo(logTime(), ': Ensuring uniqueness of UUIDs');
            // await uuidChecks(srcConn, destConn, dryRun, true);

            utils.logInfo(logTime(), ': Starting data migration ...');
            await movePersonsUsersAndAssociatedTables(srcConn, destConn);

            if(!dryRun) {
                // Copy Person & Users ID maps to the database.
                await utils.copyIdMapToDb(destConn, global.beehive.personMap, 'person');
                await utils.copyIdMapToDb(destConn, global.beehive.personAttributeTypeMap, 'person_attribute_type');
                await utils.copyIdMapToDb(destConn, global.beehive.relationshipTypeMap, 'relationship_type');
                await utils.copyIdMapToDb(destConn, global.beehive.userMap, 'users');
            }

            utils.logInfo(logTime(), ': Consolidating locations...');
            let movedLocations = await locationsMover(srcConn, destConn);

            // Copy Location ID map to the database.
            if(!dryRun) {
                await utils.copyIdMapToDb(destConn, global.beehive.locationMap, 'location');
            }
            utils.logOk(logTime(), `: Ok...${movedLocations} locations moved.`);

            //patients & identifiers
            await patientsMover(srcConn, destConn);
            if(!dryRun) {
                await utils.copyIdMapToDb(destConn, global.beehive.identifierTypeMap, 'patient_identifier_type');
            }

            //programs
            await programsMover(srcConn, destConn);

            //providers & provider attributes
            await providersMover(srcConn, destConn);
            if(!dryRun) {
                await utils.copyIdMapToDb(destConn, global.beehive.providerAttributeTypeMap, 'provider_attribute_type');
                await utils.copyIdMapToDb(destConn, global.beehive.providerMap, 'provider');
            }

            //visits & visit types
            await visitsMover(srcConn, destConn);
            if(!dryRun) {
                await utils.copyIdMapToDb(destConn, global.beehive.visitTypeMap, 'visit_type');
                await utils.copyIdMapToDb(destConn, global.beehive.visitMap, 'visit');
            }

            //encounters, encounter_types, encounter_roles & encounter_providers
            await encounterMover(srcConn, destConn);
            if(!dryRun) {
                await utils.copyIdMapToDb(destConn, global.beehive.encounterTypeMap, 'encounter_type');
                await utils.copyIdMapToDb(destConn, global.beehive.encounterRoleMap, 'encounter_role');
                await utils.copyIdMapToDb(destConn, global.beehive.encounterMap, 'encounter');

                // Record and Commit the transaction (first step)
                utils.logDebug(logTime(), ': Committing pre-obs');
                await utils.updateATransactionStep(destConn, 'pre-obs');
                await destConn.query('COMMIT');

                // Start a new transaction.
                await destConn.query('START TRANSACTION');
            }
        }

        if(dryRun || global.startingStep['atomic_step'] === 'pre-obs' ||
            (global.startingStep['atomic_step'] === 'obs' && global.startingStep['passed'] == 0 )) {
            //obs
            // Handle chunk transactions when moving obs record (This is required as the obs table
            // tends to be very big.)
            utils.logInfo(logTime(), ': Performing step: obs');
            await obsMover(srcConn, destConn);
            utils.logOk(logTime(), ': Done with moving obs here...');
            utils.logInfo(logTime(), ': Completed step: obs');

            if(!dryRun) {
                await destConn.query('START TRANSACTION');
            }
        }

        utils.logInfo(logTime(), ': Performing step: post-obs');
        //gaac tables
        await gaacModuleTablesMover(srcConn, destConn);

        utils.logInfo(logTime(), ': Completed step: post-obs');
        if(dryRun) {
            await destConn.query('ROLLBACK');
            utils.logOk(logTime(), `: Dry run done, no database changes have been made!`);
        }
        else {
            await utils.updateATransactionStep(destConn, 'post-obs');
            if(destConn) await destConn.query('COMMIT');
            utils.logOk(logTime(), `: Done...All Data from ${config.source.location} copied.`);
        }
    } catch (ex) {
        if(destConn) await destConn.query('ROLLBACK');
        utils.logError(ex);
        utils.logInfo(logTime(), ': Aborting...Rolled back, no data has been moved');
    } finally {
        if (srcConn) await srcConn.end();
        if (destConn) {
            await destConn.query('SET autocommit = 1');
            await destConn.end();
        }
        let timeElapsed = (Date.now() - startTime);
        utils.logInfo(logTime(), `: Time elapsed: ${utils.getSimpleTimeString(timeElapsed)} (${timeElapsed} ms)`);
    }
}

module.exports = orchestration; // In case one needs to require it.

// Run
orchestration();
