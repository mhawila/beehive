let utils = require('./utils');
let strValue = utils.stringValue;
let moveAllTableRecords = utils.moveAllTableRecords;

function prepareVisitTypeInsert(rows, nextId) {
  let insert = 'INSERT INTO visit_type(visit_type_id, name, description, '
        + 'creator, date_created, changed_by, date_changed, retired, retired_by,'
        + 'date_retired, retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let retiredBy = row['retired_by'] === null ? null : userMap.get(row['retired_by']);
      let changedBy = row['changed_by'] === null ? null : userMap.get(row['changed_by']);

      visitTypeMap.set(row['visit_type_id'], nextId);

      toBeinserted += `(${nextId}, ${strValue(row['name'])}, `
          + `${strValue(row['description'])}, `
          + `${userMap.get(row['creator'])}, `
          + `${strValue(utils.formatDate(row['date_created']))}, ${changedBy}, `
          + `${strValue(utils.formatDate(row['date_changed']))}, `
          + `${row['retired']}, ${retiredBy}, `
          + `${strValue(utils.formatDate(row['date_retired']))}, `
          + `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])})`;

      nextId++;
  });

  let query = insert + toBeinserted;
  return [query, nextId];
}

function prepareVisitInsert(rows, nextId) {
  let insert = 'INSERT INTO visit(visit_id, patient_id, visit_type_id, '
        + 'date_started, date_stopped, indication_concept_id, location_id, '
        + 'creator, date_created, changed_by, date_changed, voided, voided_by, '
        + 'date_voided, void_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let voidedBy = row['voided_by'] === null ? null : userMap.get(row['voided_by']);
    let changedBy = row['changed_by'] === null ? null : userMap.get(row['changed_by']);

    visitMap.set(row['visit_id'], nextId);

    toBeinserted += `(${nextId}, ${personMap.get(row['patient_id'])}, `
        + `${strValue(utils.formatDate(row['date_started']))}, `
        + `${strValue(utils.formatDate(row['date_stopped']))}, `
        + `${row['indication_concept_id']}, ${locationMap.get(row['location_id'])}, `
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

async function consolidateVisitTypes(srcConn, destConn) {
  let query = 'SELECT * FROM visit_type';
  let [srcVisitTypes] = await srcConn.query(query);
  let [destVisitTypes] = await destConn.query(query);

  let missingInDest = [];
  srcVisitTypes.forEach(srcVisitType => {
    let match = destVisitTypes.find(destVisitType => {
      return srcVisitType['name'] === destVisitType['name'];
    });

    if(match !== undefined && match !== null) {
      visityTypeMap.set(srcVisitType['visit_type_id'],
                          match['visit_type_id']);
    }
    else {
      missingInDest.push(srcVisitType);
    }
  });

  if(missingInDest.length > 0) {
    let nextVisitTypeId =
        await utils.getNextAutoIncrementId(destConn, 'visit_type');

    let [sql] = prepareVisitTypeInsert(missingInDest, nextVisitTypeId);
    let [result] = await destConn.query(sql);
  }
}

async function moveVisits(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'visit', 'visit_id',
                  prepareVisitInsert);
}
