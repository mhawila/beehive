let utils = require('./utils');
let strValue = utils.stringValue;
let moveAllTableRecords = utils.moveAllTableRecords;

function prepareObsInsert(rows, nextId) {
  let insert = 'INSERT INTO obs(obs_id, person_id, concept_id, encounter_id, '
        + 'order_id, obs_datetime, location_id, obs_group_id, accession_number, '
        + 'value_group_id, value_boolean, value_coded, value_coded_name_id, '
        + 'value_drug, value_datetime, value_numeric, value_modifier, '
        + 'value_text, value_complex, comments, previous_version, creator, '
        + 'date_created, changed_by, date_changed, voided, voided_by, '
        + 'date_voided, void_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let voidedBy = row['voided_by'] === null ? null : userMap.get(row['voided_by']);
    let changedBy = row['changed_by'] === null ? null : userMap.get(row['changed_by']);

    obsMap.set(row['obs_id'], nextId);

    toBeinserted += `(${nextId}, ${personMap.get(row['person_id'])}, `
        + `${row['concept_id']},  ${encounterMap.get(row['encounter_id'])}, `
        + `${row['order_id']}, ${strValue(utils.formatDate(row['obs_datetime']))}, `
        + `${locationMap.get(row['location_id'])}, ${obsMap.get(row['obs_group_id'])}, `
        + `${strValue(row['accession_number'])}, ${row['value_group_id']}, `
        + `${row['value_boolean']}, ${row['value_coded']}, `
        + `${row['value_coded_name_id']}, ${row['value_drug']}, `
        + `${strValue(utils.formatDate(row['value_datetime']))}, `
        + `${row['value_numeric']}, ${strValue(row['value_modifier'])}, `
        + `${strValue(row['value_text'])}, ${strValue(row['value_complex'])}, `
        + `${strValue(row['comments'])}, ${obsMap.get(row['previous_version'])}, `
        + `${userMap.get(row['creator'])}, `
        + `${strValue(utils.formatDate(row['date_created']))}, `
        + `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, `
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
