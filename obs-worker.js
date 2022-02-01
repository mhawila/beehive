const { parentPort, workerData } = require('worker_threads');
const connection = require('./connection').connection;
const utils = require('./utils');
const config = require('./config');

function prepareObsInsert(rows, nextId) {
    let insert = 'INSERT IGNORE INTO obs(obs_id, person_id, concept_id, encounter_id, '
          + 'order_id, obs_datetime, location_id, obs_group_id, accession_number, '
          + 'value_group_id, value_boolean, value_coded, value_coded_name_id, '
          + 'value_drug, value_datetime, value_numeric, value_modifier, '
          + 'value_text, value_complex, comments, previous_version, creator, '
          + 'date_created, voided, voided_by, '
          + 'date_voided, void_reason, uuid, form_namespace_and_path) VALUES ';
  
    let toBeinserted = '';
    for(let i = 0; i < rows.length; i++) {
        let row = rows[i];
        if(!global.excludedObsIds.includes(row['obs_id'])) {
            if(toBeinserted.length > 1) {
                toBeinserted += ',';
            }
    
            let voidedBy = row['voided_by'] === null ? null : workerData.userMap[row['voided_by']];
            let obsGroupsId = row['obs_group_id'] === null ? null : workerData.obsMap[row['obs_group_id']];
            let previous = row['previous_version']=== null ? null : workerData.obsMap[row['previous_version']];
            let encounterId = row['encounter_id'] === null ? null : workerData.encounterMap[row['encounter_id']];
            let locationId = row['location_id'] === null ? null : workerData.locationMap[row['location_id']];
            workerData.obsMap[row['obs_id']] = nextId;
            
            if(obsGroupsId === undefined) {
                // The new value of obs_group_id is not yet known because the associated obs is not yet copied.
                obsGroupsId = null;
                if(row['obs_group_id'] !== null) {
                    obsWithTheirGroupNotUpdated[nextId] = row['obs_group_id'];
                }
            }
        
            if(previous === undefined) {
                previous = null;
                if(row['previous_version'] !== null) {
                    obsWithPreviousNotUpdated[nextId] = row['previous_version'];
                }
            }
    
            toBeinserted += `(${nextId}, ${workerData.personMap[row['person_id']]}, `
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
                + `${workerData.userMap[row['creator']]}, `
                + `${strValue(utils.formatDate(row['date_created']))}, `
                + `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, `
                + `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])}, `
                + `${strValue(row['form_namespace_and_path'])})`;
        
            nextId++;
        }
    }

    if(toBeinserted === '') {
        return [null, nextId];
    }

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

async function moveObs(srcConn, destConn, start, countToMove) {
    let condition = null;
    if(global.excludedObsIds.length > 0) {
        condition = `obs_id NOT IN (${global.excludedObsIds.join(',')})`;
    }
    return await moveAllTableRecords(srcConn, destConn, 'obs', 'date_created',
                  prepareObsInsert, condition, start, countToMove);
}

async function updateObsPreviousOrGroupId(connection, idMap, field) {
    let mapEntries = Object.entries(idMap);
    if(mapEntries.length > 0) {
        let update = `INSERT INTO obs(obs_id, ${field}) VALUES `;
        let lastPart = ` ON DUPLICATE KEY UPDATE ${field} = VALUES(${field})`;

        let values = '';
        for(const [obsId, srcIdValue] of mapEntries) {
            if(values.length > 1) {
                values += ',';
            }
            values += `(${obsId}, ${workerData.obsMap[srcIdValue]})`;
        }

        let query = update + values + lastPart;
        utils.logDebug(`${field} update query:`, query);
        await connection.query(query);
    }
    return mapEntries.length;
}

async function copyObs(start, countToMove, nextAutoIncrementId, threadId) {
    let dryRun = process.argv.some(arg => (arg === '--dry-run'));
    let startTime = Date.now();
    let srcConn = null;
    let destConn = null;
    try {
        utils.logDebug(`Worker thread #${workerData.threadId}`)
        srcConn = await connection(config.source);
        destConn = await connection(config.destination);
        
        utils.logInfo(utils.logTime(), `: Starting obs migration in thread #${threadId} ...`);
        destConn.query('START TRANSACTION');
        let copied = await moveObs(srcConn, destConn, start, countToMove, nextAutoIncrementId);

        if(dryRun) {
            destConn.query('ROLLBACK');
            utils.logOk(`Done... Worker thread #${threadId} did not make any changes to the database!`)
        }
        else {
            destConn.query('COMMIT');
            utils.logOk(`Done... Worker thread #${threadId} finished copying ${countToMove} obs records`);
        }
        parentPort.postMessage(copied);
    } catch (ex) {
        if(destConn) {
            destConn.query('ROLLBACK');
        }
        utils.logError(ex);
        utils.logInfo('Aborting worker thread... Rolled back, no data has been copied');
    } finally {
        if (srcConn) srcConn.end();
        if (destConn) destConn.end();
        let timeElapsed = (Date.now() - startTime);
        utils.logInfo(`Worker thread duration: ${timeElapsed} ms`);
    }
}

copyObs(workerData.start, workerData.countToMove, workerData.nextId, workerData.threadId);