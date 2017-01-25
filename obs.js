let utils = require('./utils');
let strValue = utils.stringValue;
let moveAllTableRecords = utils.moveAllTableRecords;

let beehive = global.beehive;

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
    beehive.obsMap.set(row['obs_id'], nextId);

    toBeinserted += `(${nextId}, ${beehive.personMap.get(row['person_id'])}, `
        + `${row['concept_id']},  ${beehive.encounterMap.get(row['encounter_id'])}, `
        + `${row['order_id']}, ${strValue(utils.formatDate(row['obs_datetime']))}, `
        + `${beehive.locationMap.get(row['location_id'])}, ${obsGroupsId}, `
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

module.exports = async function(srcConn, destConn) {
    utils.logInfo('Moving obs...');
    let srcObsCount = await utils.getCount(srcConn, 'obs');
    let initialDestCount = await utils.getCount(destConn, 'obs');
    let expectedFinalCount = initialDestCount + srcObsCount;

    let moved = await moveObs(srcConn, destConn);
    let finalDestCount = await utils.getCount(destConn, 'obs');

    if (finalDestCount === expectedFinalCount) {
        utils.logOk(`Ok... ${moved} obs moved.`);
    } else {
        let error = `Problem moving obs: the actual final count ` +
            `(${expectedFinalCount}) is not equal to the expected value ` +
            `(${finalDestCount})`;
        throw new Error(error);
    }
}
