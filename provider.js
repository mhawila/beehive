let utils = require('./utils');
let strValue = utils.stringValue;
let moveAllTableRecords = utils.moveAllTableRecords;

function prepareProviderInsert(rows, nextId) {
  let insert = 'INSERT INTO provider(provider_id, person_id, name, identifier, '
        + 'creator, date_created, changed_by, date_changed, retired, retired_by, '
        + 'date_retired, retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let retiredBy = row['retired_by'] === null ? null : userMap.get(row['retired_by']);
      let changedBy = row['changed_by'] === null ? null : userMap.get(row['changed_by']);

      providerMap.set(row['provider_id'], nextId);
      toBeinserted += `(${nextId}, ${personMap.get(row['person_id'])}, `
          + `${strValue(row['name'])}, ${strValue(row['identifier'])}, `
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

function prepareProviderAttributeTypeInsert(rows, nextId) {
  let insert = 'INSERT INTO provider_attribute_type(provider_attribute_type_id, '
        + 'name, description, datatype, datatype_config, preferred_handler, '
        + 'handler_config, min_occurs, max_occurs, creator, date_created, '
        + 'changed_by, date_changed, retired, retired_by, date_retired, '
        + 'retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let retiredBy = row['retired_by'] === null ? null : userMap.get(row['retired_by']);
      let changedBy = row['changed_by'] === null ? null : userMap.get(row['changed_by']);

      providerAttributeTypeMap.set(row['provider_attribute_type_id'], nextId);
      toBeinserted += `(${nextId}, ${strValue(row['name'])}, `
          + `${strValue(row['description'])}, ${strValue(row['datatype'])}, `
          + `${strValue(row['datatype_config'])}, ${strValue(row['preferred_handler'])}, `
          + `${strValue(row['handler_config'])}, ${row['min_occurs']}, ${row['max_occurs']}`
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

function prepareProviderAttributeInsert(rows, nextId) {
  let insert = 'INSERT INTO provider_attribute(provider_attribute_id, '
        + 'provider_id, attribute_type_id, value_reference, creator, '
        + 'date_created, changed_by, date_changed, voided, voided_by, '
        + 'date_voided, void_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let voidedBy = row['voided_by'] === null ? null : userMap.get(row['voided_by']);
    let changedBy = row['changed_by'] === null ? null : userMap.get(row['changed_by']);

    toBeinserted += `(${nextId}, ${providerMap.get(row['provider_id'])}, `
        + `${providerAttributeTypeMap.get(row['attribute_type_id'])}, `
        + `${strValue(row['value_reference'])}, `
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

async function consolidateProviderAttributeTypes(srcConn, destConn) {
  let query = 'SELECT * FROM person_attribute_type';
  let [srcProvAttTypes] = await srcConn.query(query);
  let [destProvAttTypes] = await destConn.query(query);

  let missingInDest = [];
  srcProvAttTypes.forEach(srcProvAttType => {
    let match = destProvAttTypes.find(destProvAttType => {
      return srcProvAttType['name'] === destProvAttType['name'];
    });

    if(match !== undefined && match !== null) {
      providerAttributeTypeMap.set(srcProvAttType['provider_attribute_type_id'],
                          match['provider_attribute_type_id']);
    }
    else {
      missingInDest.push(srcProvAttType);
    }
  });

  if(missingInDest.length > 0) {
    let nextProvAttTypeId =
        await utils.getNextAutoIncrementId(destConn, 'provider_attribute_type');

    let [sql] = prepareProviderAttributeTypeInsert(missingInDest, nextProvAttTypeId);
    let [result] = await destConn.query(sql);
  }
}

async function moveProviders(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'provider', 'provider_id',
                  prepareProviderInsert);
}

async function moveProviderAttributes(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'provider_attribute',
                'provider_attribute_id', prepareProviderAttributeInsert);
}
