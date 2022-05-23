const { addDecimalNumbers, subtractDecimalNumbers, shortenInsert, copyAllTableRecords: moveAllTableRecords } = require('./utils');
const config = require('./config');
let utils = require('./utils');
let strValue = utils.stringValue;
const prettyPrintRows = require('./display-utils').prettyPrintRows;

let beehive = global.beehive;
let obsWithTheirGroupNotUpdated = {};
let obsWithPreviousNotUpdated = {};

function prepareObsInsert(rows, nextId) {
    let insert = 'INSERT IGNORE INTO obs(obs_id, person_id, concept_id, encounter_id, '
            + 'order_id, obs_datetime, location_id, obs_group_id, accession_number, '
            + 'value_group_id, value_coded, value_coded_name_id, '
            + 'value_drug, value_datetime, value_numeric, value_modifier, '
            + 'value_text, value_complex, comments, previous_version, creator, '
            + 'date_created, voided, voided_by, '
            + 'date_voided, void_reason, uuid, form_namespace_and_path, ';
            
    if(global.openmrsDataModelVersion === 2) {
        insert += 'status, interpretation) VALUES ';
    } else {
        insert += 'value_boolean) VALUES ';
    }

    let toBeinserted = '';
    for(let i = 0; i < rows.length; i++) {
        let row = rows[i];

        if(!global.excludedObsIds.includes(row['obs_id'])) {
            if(toBeinserted.length > 1) {
                toBeinserted += ',';
            }
    
            let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
            let obsGroupsId = row['obs_group_id'] === null ? null : beehive.obsMap[row['obs_group_id']];
            let previous = row['previous_version']=== null ? null : beehive.obsMap[row['previous_version']];
            let encounterId = row['encounter_id'] === null ? null : beehive.encounterMap.get(row['encounter_id']);
            let locationId = row['location_id'] === null ? null : beehive.locationMap.get(row['location_id']);
            beehive.obsMap[row['obs_id']] = nextId;
        
            if(obsGroupsId === undefined) {
                obsGroupsId = null;
                if(row['obs_group_id'] !== null) {
                    // obsWithPreviousNotUpdated[nextId] = row['obs_group_id'];
                    obsWithTheirGroupNotUpdated[row['obs_id']] = {
                        destObsId: nextId,
                        toBeUpdated: row['obs_group_id']
                    };
                }
            }
        
            if(previous === undefined) {
                previous = null;
                if(row['previous_version'] !== null) {
                    // obsWithPreviousNotUpdated[nextId] = row['previous_version'];
                    obsWithPreviousNotUpdated[row['obs_id']] = {
                        destObsId: nextId,
                        toBeUpdated: row['previous_version']
                    };
                }
            }
    
            toBeinserted += `(${nextId}, ${beehive.personMap.get(row['person_id'])}, `
                + `${row['concept_id']},  ${encounterId}, `
                + `${row['order_id']}, ${strValue(utils.formatDate(row['obs_datetime']))}, `
                + `${locationId}, ${obsGroupsId}, `
                + `${strValue(row['accession_number'])}, ${row['value_group_id']}, `
                + `${row['value_coded']}, `
                + `${row['value_coded_name_id']}, ${row['value_drug']}, `
                + `${strValue(utils.formatDate(row['value_datetime']))}, `
                + `${row['value_numeric']}, ${strValue(row['value_modifier'])}, `
                + `${strValue(row['value_text'])}, ${strValue(row['value_complex'])}, `
                + `${strValue(row['comments'])}, ${previous}, `
                + `${beehive.userMap.get(row['creator'])}, `
                + `${strValue(utils.formatDate(row['date_created']))}, `
                + `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, `
                + `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])}, `
                + `${strValue(row['form_namespace_and_path'])}, `;
            
                if(global.openmrsDataModelVersion === 2) {
                    toBeinserted +=  `${strValue(row['status'])}, ${strValue(row['interpretation'])})`;
                } else {
                    toBeinserted += `${row['value_boolean']})`;
                }

            nextId++;
        }
    }

    if(toBeinserted === '') {
        return [null, nextId];
    }

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

async function moveObs(srcConn, destConn) {
    let condition = null;
    if(global.excludedObsIds.length > 0) {
        condition = `obs_id NOT IN (${global.excludedObsIds.join(',')})`;
    }
    return await moveAllTableRecords(srcConn, destConn, 'obs', 'date_created',
                  prepareObsInsert, condition);
}

async function updateObsPreviousOrGroupId(connection, idMap, field) {
    let mapEntries = Object.entries(idMap);
    if(mapEntries.length > 0) {
        utils.logInfo(`Updating obs.${field} for ${mapEntries.length} records`);
        let update = `INSERT INTO obs(obs_id, ${field}) VALUES `;
        let lastPart = ` ON DUPLICATE KEY UPDATE ${field} = VALUES(${field})`;

        let start = 0;
        let temp = mapEntries.length;
        let sql;
        let queryLogged = false;
        let limit = null;
        try {
            while (temp % config.batchSize > 0) {
                if (Math.floor(temp / config.batchSize) > 0) {
                    limit = addDecimalNumbers(start, config.batchSize);
                    temp = subtractDecimalNumbers(temp, config.batchSize);
                    
                } else {
                    limit = addDecimalNumbers(start, temp);
                    temp = 0;
                }

                let values = '', obsId, srcIdValue;
                for(let i=start; i < limit; i++) {
                    if(values.length > 1) {
                        values += ',';
                    }
                    obsId = mapEntries[i][1]['destObsId'];
                    srcIdValue = mapEntries[i][1]['toBeUpdated'];
                    values += `(${obsId}, ${beehive.obsMap[srcIdValue]})`;
                }

                sql = update + values + lastPart;
                if (!queryLogged) {
                    utils.logDebug(`First obs.${field} update statement:\n`, shortenInsert(sql));
                    queryLogged = true;
                }
                await connection.query(sql);
                start = addDecimalNumbers(start, config.batchSize);
            }
            return mapEntries.length;
        }
        catch(ex) {
            utils.logError(`An error occured when updating obs ${field} column`);
            if(sql) {
                utils.logError('Statement during error');
                utils.logError(sql);
            }
            utils.logDebug(`Obs whose ${field} were being updated during the error.`);
            let headers = ['source obs_id', `source ${field}`, 'dest obs_id', `dest ${field}`];
            let rows = [];
            let srcObsIds = [];
            for(let i=start; i<limit; i++) {
                srcObsIds.push(mapEntries[i][0]);
                rows.push([mapEntries[i][0], mapEntries[i][1]['toBeUpdated'], mapEntries[i][1]['destObsId'], beehive.obsMap[mapEntries[i][1]['toBeUpdated']]]);
            }
            prettyPrintRows(rows, headers);

            // In case some of the obs whose obs_group_id/previous_version are to be updated do not exist in the destination.
            let query = `SELECT obs_id FROM ${config.source.openmrsDb}.obs WHERE obs_id IN (${srcObsIds.join(',')})`;
            let [records] = await connection.query(query);
            records.forEach(row => {
                let index = srcObsIds.findIndex(obsId => obsId == row['obs_id']);
                if(index >= 0) {
                    srcObsIds.splice(index, 1);
                }
            });
            if(srcObsIds.length > 0) {
                utils.logDebug(`ATTENTION: System attempted to update ${field} for obs which were not present in destination. Source obs_id for these obs are:\n`);
                utils.logDebug(`${srcObsIds.join(',')}`);
            } else {
                utils.logDebug(`All obs which are being updated already exist on the destination system!`);
            }
            throw ex;
        }
    }    
}

module.exports = async function(srcConn, destConn) {
    utils.logInfo('Moving obs...');
    let srcObsCount = await utils.getCountIgnoringDestinationDuplicateUuids(srcConn, 'obs');
    let initialDestCount = await utils.getCount(destConn, 'obs');
    let expectedFinalCount = initialDestCount + srcObsCount;

    let moved = await moveObs(srcConn, destConn);
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
