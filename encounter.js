let utils = require('./utils');
let strValue = utils.stringValue;
let moveAllTableRecords = utils.moveAllTableRecords;

let beehive = global.beehive;

function prepareEncounterRoleInsert(rows, nextId) {
  let insert = 'INSERT INTO encounter_role(encounter_role_id, name, description, '
        + 'creator, date_created, changed_by, date_changed, retired, retired_by,'
        + 'date_retired, retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
      let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

      beehive.encounterRoleMap.set(row['encounter_role_id'], nextId);

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

function prepareEncounterProviderInsert(rows, nextId) {
  let insert = 'INSERT INTO encounter_provider(encounter_provider_id, '
        + 'encounter_id, provider_id, encounter_role_id, creator, date_created, '
        + 'changed_by, date_changed, voided, voided_by, date_voided, '
        + 'void_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
    let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

    toBeinserted += `(${nextId}, ${encounterMap.get(row['encounter_id'])}, `
        + `${providerMap.get(row['provider_id'])}, `
        + `${encounterRoleMap.get(row['encounter_role_id'])}, `
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

function prepareEncounterTypeInsert(rows, nextId) {
  let insert = 'INSERT INTO encounter_type(encounter_type_id, name, '
       + 'description, creator, date_created, retired, retired_by, '
       + 'date_retired, retire_reason, uuid) VALUES ';

 let toBeinserted = '';
 rows.forEach(row => {
     if (toBeinserted.length > 1) {
         toBeinserted += ',';
     }

     let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);

     beehive.encounterTypeMap.set(row['encounter_type_id'], nextId);

     toBeinserted += `(${nextId}, ${strValue(row['name'])}, `
         + `${strValue(row['description'])}, `
         + `${userMap.get(row['creator'])}, `
         + `${strValue(utils.formatDate(row['date_created']))}, `
         + `${row['retired']}, ${retiredBy}, `
         + `${strValue(utils.formatDate(row['date_retired']))}, `
         + `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])})`;

     nextId++;
 });

 let query = insert + toBeinserted;
 return [query, nextId];
}

function prepareEncounterInsert(rows, nextId) {
  let insert = 'INSERT INTO encounter(encounter_id, encounter_type, patient_id, '
        + 'location_id, form_id, visit_id, encounter_datetime, creator, date_created, '
        + 'changed_by, date_changed, voided, voided_by, date_voided, '
        + 'void_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
    let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

    beehive.encounterMap.set(row['encounter_id'], nextId);

    toBeinserted += `(${nextId}, ${encounterTypeMap.get(row['encounter_type'])}, `
        + `${personMap.get(row['patient_id'])}, `
        + `${locationMap.get(row['location_id'])}, ${row['form_id']}, `
        + `${visitMap.get(row['visit_id'])}, `
        + `${strValue(utils.formatDate(row['encounter_datetime']))}, `
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

async function consolidateEncounterTypes(srcConn, destConn) {
  let query = 'SELECT * FROM encounter_type';
  let [srcEncounterTypes] = await srcConn.query(query);
  let [destEncounterTypes] = await destConn.query(query);

  let missingInDest = [];
  srcEncounterTypes.forEach(srcEncounterType => {
    let match = destEncounterTypes.find(destEncounterType => {
      return srcEncounterType['name'] === destEncounterType['name'];
    });

    if(match !== undefined && match !== null) {
      beehive.encounterTypeMap.set(srcEncounterType['encounter_type_id'],
                          match['encounter_type_id']);
    }
    else {
      missingInDest.push(srcEncounterType);
    }
  });

  if(missingInDest.length > 0) {
    let nextEncounterTypeId =
        await utils.getNextAutoIncrementId(destConn, 'encounter_type');

    let [sql] = prepareEncounterTypeInsert(missingInDest, nextEncounterTypeId);
    let [result] = await destConn.query(sql);
  }
}

async function consolidateEncounterRoles(srcConn, destConn) {
  let query = 'SELECT * FROM encounter_role';
  let [srcEncounterRoles] = await srcConn.query(query);
  let [destEncounterRoles] = await destConn.query(query);

  let missingInDest = [];
  srcEncounterRoles.forEach(srcEncounterRole => {
    let match = destEncounterRoles.find(destEncounterRole => {
      return srcEncounterRole['name'] === destEncounterRole['name'];
    });

    if(match !== undefined && match !== null) {
      beehive.encounterRoleMap.set(srcEncounterRole['encounter_role_id'],
                          match['encounter_role_id']);
    }
    else {
      missingInDest.push(srcEncounterRole);
    }
  });

  if(missingInDest.length > 0) {
    let nextEncounterRoleId =
        await utils.getNextAutoIncrementId(destConn, 'encounter_role');

    let [sql] = prepareEncounterRoleInsert(missingInDest, nextEncounterRoleId);
    let [result] = await destConn.query(sql);
  }
}

async function moveEncounters(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'encounter',
                'encounter_id', prepareEncounterInsert);
}

async function moveEncounterProviders(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'encounter_provider',
                'encounter_provider_id', prepareEncounterProviderInsert);
}
