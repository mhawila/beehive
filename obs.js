let utils = require('./utils');
let strValue = utils.stringValue;
let moveAllTableRecords = utils.moveAllTableRecords;

let beehive = global.beehive;

const config = require('./config');

if (config.batchSize === undefined) {
    config.batchSize = 500;
};

let obsWithTheirGroupNotUpdated = new Map();
let obsWithPreviousNotUpdated = new Map();
const MIN_COUNT_FOR_OBS_TRANSACTION = 500000;
const OBS_TRANSACTION_BATCH_SIZE = 250000;
const TEMP_OBS_MAP = new Map();

function prepareObsInsert(rows, nextId) {
  let insert = 'INSERT INTO obs(obs_id, person_id, concept_id, encounter_id, '
        + 'order_id, obs_datetime, location_id, obs_group_id, accession_number, '
        + 'value_group_id, value_boolean, value_coded, value_coded_name_id, '
        + 'value_drug, value_datetime, value_numeric, value_modifier, '
        + 'value_text, value_complex, comments, previous_version, creator, '
        + 'date_created, voided, voided_by, '
        + 'date_voided, void_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }

    let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
    let obsGroupsId = row['obs_group_id'] === null ? null : beehive.obsMap.get(row['obs_group_id']);
    let previous = row['previous_version']=== null ? null : beehive.obsMap.get(row['previous_version']);
    let encounterId = row['encounter_id'] === null ? null : beehive.encounterMap.get(row['encounter_id']);
    let locationId = row['location_id'] === null ? null : beehive.locationMap.get(row['location_id']);

    TEMP_OBS_MAP.set(row['obs_id'], nextId);

    if(obsGroupsId === undefined) {
        obsGroupsId = null;
        if(row['obs_group_id'] !== null) {
            obsWithTheirGroupNotUpdated.set(nextId, row['obs_group_id']);
        }
    }

    if(previous === undefined) {
        previous = null;
        if(row['previous_version'] !== null) {
            obsWithPreviousNotUpdated.set(nextId, row['previous_version']);
        }
    }

    toBeinserted += `(${nextId}, ${beehive.personMap.get(row['person_id'])}, `
        + `${row['concept_id']},  ${encounterId}, `
        + `${row['order_id']}, ${strValue(utils.formatDate(row['obs_datetime']))}, `
        + `${locationId}, ${obsGroupsId}, `
        + `${strValue(row['accession_number'])}, ${row['value_group_id']}, `
        + `${row['value_boolean']}, ${row['value_coded']}, `
        + `${row['value_coded_name_id']}, ${row['value_drug']}, `
        + `${strValue(utils.formatDate(row['value_datetime']))}, `
        + `${row['value_numeric']}, ${strValue(row['value_modifier'])}, `
        + `${strValue(row['value_text'])}, ${strValue(row['value_complex'])}, `
        + `${strValue(row['comments'])}, ${previous}, `
        + `${beehive.userMap.get(row['creator'])}, `
        + `${strValue(utils.formatDate(row['date_created']))}, `
        + `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, `
        + `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`

    nextId++;
  });

  let insertStatement = insert + toBeinserted;
  return [insertStatement, nextId];
}

async function moveObs(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'obs', 'date_created',
                  prepareObsInsert);
}

/**
 * Utility function that moves all obs records in config.batchSize batches
 * @param srcConn
 * @param destConn
 * @return count of records moved. (or a promise that resolves to count)
 */
 async function moveAllObs(srcConn, destConn, condition) {
    // Get the count to be pushed
    let tableName = 'obs';
    let orderColumn = 'date_created';
    let countToMove = await utils.getCount(srcConn, tableName, condition);
    let nextAutoIncr = await utils.getNextAutoIncrementId(destConn, tableName);

    let fetchQuery = `SELECT * FROM ${tableName} `;
    if(condition) {
        fetchQuery += `WHERE  ${condition} `;
    }
    fetchQuery += `ORDER by ${orderColumn} LIMIT `;
    let start = 0;
    let temp = countToMove;
    let moved = 0;
    let queryLogged = false;
    let query = null;
    let [q, nextId] = [null, -1];
    try {
        if(!global.dryRun) {
            utils.logInfo(utils.logTime(), ': Moving obs in transaction');

            if(global.startingStep['atomic_step'] === 'obs' && global.startingStep['moved_records'] > 0) {
                moved = start = global.startingStep['moved_records'];
            }
            await destConn.query('START TRANSACTION');
        }
        while (temp > 0) {
            query = fetchQuery;
            if (Math.floor(temp / config.batchSize) > 0) {
                // moved += config.batchSize;
                query += start + ', ' + config.batchSize;
                temp -= config.batchSize;
            } else {
                // moved += temp;
                query += start + ', ' + temp;
                temp = 0;
            }
            start += config.batchSize;
            let [r] = await srcConn.query(query);
            [q, nextId] = prepareObsInsert(r, nextAutoIncr);

            if (!queryLogged) {
                utils.logDebug(`${tableName} insert statement:\n`, utils.shortenInsert(q));
                queryLogged = true;
            }

            if(q) {
                [r] = await destConn.query(q);
                moved += r.affectedRows;

                if(!global.dryRun && countToMove >= MIN_COUNT_FOR_OBS_TRANSACTION) {
                    if(moved > 0 && moved%OBS_TRANSACTION_BATCH_SIZE === 0) {
                        await utils.copyIdMapToDb(destConn, TEMP_OBS_MAP, 'obs');

                        // Copy to the global map fo use later.
                        for(let obsMapItem of TEMP_OBS_MAP[Symbol.iterator]()) {
                            beehive.obsMap.set(obsMapItem[0], obsMapItem[1]);
                        }

                        // Clear the temp map making it ready for next transaction.
                        TEMP_OBS_MAP.clear();

                        // Update transaction progress.
                        await utils.updateATransactionStep(destConn, 'obs', 0, moved);
                        await destConn.query('COMMIT');

                        utils.logDebug(utils.logTime(), ': obs transaction committed, obs moved --> ', moved);
                        await destConn.query('START TRANSACTION');
                    }
                }
            }

            nextAutoIncr = nextId;
        }

        // Deal with the final batch for transaction-wise moves.
        if(!global.dryRun) {
            if(TEMP_OBS_MAP.size > 0) {
                await utils.copyIdMapToDb(destConn, TEMP_OBS_MAP, 'obs');
                await utils.updateATransactionStep(destConn, 'obs', 1, moved);

                await destConn.query('COMMIT');

                utils.logDebug(utils.logTime(), ': obs transaction committed, obs moved --> ', moved);
            }
        }
        utils.logInfo(utils.logTime(), ': Total obs records moved --> ', moved);
        // Copy to the global map fo use later. This works for both the final batch if it is a transaction-wise
        // move or otherwise.
        for(let obsMapItem of TEMP_OBS_MAP[Symbol.iterator]()) {
            beehive.obsMap.set(obsMapItem[0], obsMapItem[1]);
        }

        // Clear the temp map free space.
        TEMP_OBS_MAP.clear();
        return moved;
    }
    catch(ex) {
        if(!global.dryRun) {
            // ROLLBACK
            let committedObs = 0;
            if(countToMove >= MIN_COUNT_FOR_OBS_TRANSACTION) {
                if(moved%OBS_TRANSACTION_BATCH_SIZE > 0) {
                    committedObs = moved - moved%OBS_TRANSACTION_BATCH_SIZE;
                } else {
                    committedObs = moved - OBS_TRANSACTION_BATCH_SIZE;
                }
            }
            utils.logDebug('Number of moved committed obs during error: ', committedObs);
            utils.logInfo(utils.logTime(), ': Rolling back current obs move transaction because of error');
            await destConn.query('ROLLBACK');
        }
        utils.logError(`An error occured when moving ${tableName} records`);
        if(q) {
            utils.logError('Select statement:', query);
            utils.logError('Insert statement during error');
            utils.logError(q);
        }
        throw ex;
    }
}

async function updateObsPreviousOrGroupId(connection, idMap, field) {
    if(idMap.size > 0) {
        let update = `INSERT INTO obs(obs_id, ${field}) VALUES `;
        let lastPart = ` ON DUPLICATE KEY UPDATE ${field} = VALUES(${field})`;

        let values = '';
        idMap.forEach((srcIdValue, obsId) => {
            if(values.length > 1) {
                values += ',';
            }
            values += `(${obsId}, ${beehive.obsMap.get(srcIdValue)})`;
        });

        let query = update + values + lastPart;
        utils.logDebug(`${field} update query:`, query);
        await connection.query(query);
    }
    return idMap.size;
}

module.exports = async function(srcConn, destConn) {
    utils.logInfo('Moving obs...');
    let srcObsCount = await utils.getCount(srcConn, 'obs');
    let initialDestCount = await utils.getCount(destConn, 'obs');
    let expectedFinalCount = initialDestCount + srcObsCount;

    let moved = await moveAllObs(srcConn, destConn);
    let finalDestCount = await utils.getCount(destConn, 'obs');

    if (finalDestCount === expectedFinalCount) {
        // Update obs_group_id & previous_version for records not yet updated
        await updateObsPreviousOrGroupId(destConn, obsWithTheirGroupNotUpdated, 'obs_group_id');
        await updateObsPreviousOrGroupId(destConn, obsWithPreviousNotUpdated, 'previous_version');
        utils.logOk(`Ok... ${moved} obs moved.`);
    } else {
        let error = `Problem moving obs: the actual final count ` +
            `(${expectedFinalCount}) is not equal to the expected value ` +
            `(${finalDestCount})`;
        throw new Error(error);
    }
}
