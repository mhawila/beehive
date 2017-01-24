'use strict';
const connection = require('./connection').connection;
const utils = require('./utils');
const strValue = utils.stringValue;
const uuid = utils.uuid;
const logTime = utils.logTime;
const moveAllTableRecords = utils.moveAllTableRecords;
const config = require('./config');

let beehive = global.beehive;

function preparePersonInsert(rows, nextPersonId) {
    let insert = 'INSERT INTO person(person_id, gender, birthdate,' +
        'birthdate_estimated, dead, death_date, cause_of_death, creator,' +
        'date_created, changed_by, date_changed, voided, voided_by,' +
        'date_voided, void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        beehive.personMap.set(row['person_id'], nextPersonId);
        toBeinserted += `(${nextPersonId}, '${row['gender']}', `
            + `${strValue(utils.formatDate(row['birthdate']))},`
            + `${strValue(row['birthdate_estimated'])}, ${row['dead']},`
            + `${strValue(utils.formatDate(row['deathdate']))}, `
            + `${strValue(row['cause_of_death'])}, ${userMap.get(row['creator'])}, `
            + `${strValue(utils.formatDate(row['date_created']))},`
            + `${row['changed_by']}, ${strValue(utils.formatDate(row['date_changed']))},`
            + `${row['voided']}, ${row['voided_by']},`
            + `${strValue(utils.formatDate(row['date_voided']))},`
            + `${strValue(row['void_reason'])}, ${uuid(row['uuid'])})`;
        nextPersonId++;
    })

    let query = insert + toBeinserted;

    return [query, nextPersonId];
}

function preparePersonNameInsert(rows, nextPersonNameId) {
  let insert = 'INSERT INTO person_name(person_name_id, preferred, person_id, '
             + 'prefix, given_name, middle_name, family_name_prefix, family_name,'
             + 'family_name2, family_name_suffix, degree, creator, date_created,'
             + 'voided, voided_by, date_voided, void_reason, changed_by, '
             + 'date_changed, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let currentPersonId = beehive.personMap.get(row['person_id']);
    if(currentPersonId !== undefined) {
      let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
      let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

      toBeinserted += `(${nextPersonNameId}, ${row['preferred']}, ${currentPersonId}, `
          + `${strValue(row['prefix'])}, ${strValue(row['given_name'])}, `
          + `${strValue(row['middle_name'])}, ${strValue(row['family_name_prefix'])}, `
          + `${strValue(row['family_name'])}, ${strValue(row['family_name2'])}, `
          + `${strValue(row['family_name_suffix'])}, ${strValue(row['degree'])}, `
          + `${userMap.get(row['creator'])}, ${strValue(utils.formatDate(row['date_created']))}, `
          + `${row['voided']}, ${voidedBy}, `
          + `${strValue(utils.formatDate(row['date_voided']))}, ${strValue(row['void_reason'])}, `
          + `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, `
          + `${uuid(row['uuid'])})`;
      nextPersonNameId++;
    }
  });
  let query = null;
  if(toBeinserted !== '') query = insert + toBeinserted;

  return [query, nextPersonNameId];
}

function preparePersonAddressInsert(rows, nextId) {
  let insert = 'INSERT INTO person_address (person_address_id, person_id, '
        + 'preferred, address1, address2, city_village, state_province, '
        + 'postal_code, country, latitude, longitude, creator, date_created, '
        + 'voided, voided_by, date_voided, void_reason, county_district, '
        + 'address3, address6, address5, address4, uuid, date_changed, '
        + 'changed_by, start_date, end_date) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
    let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

    toBeinserted += `(${nextId}, ${personMap.get(row['person_id'])}, `
        + `${row['preferred']}, ${strValue(row['address1'])}, `
        + `${strValue(row['address2'])}, ${strValue(row['city_village'])}, `
        + `${strValue(row['state_province'])}, ${strValue(row['postal_code'])}, `
        + `${strValue(row['country'])}, ${strValue(row['latitude'])}, `
        + `${strValue(row['longitude'])}, ${userMap.get(row['creator'])}, `
        + `${strValue(utils.formatDate(row['date_created']))}, ${row['voided']}, `
        + `${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, `
        + `${strValue(row['void_reason'])}, ${strValue(row['county_district'])}, `
        + `${strValue(row['address3'])}, ${strValue(row['address6'])}, `
        + `${strValue(row['address5'])}, ${strValue(row['address4'])}, `
        + `${uuid(row['uuid'])}, ${strValue(utils.formatDate(row['date_changed']))}, `
        + `${changedBy}, ${strValue(utils.formatDate(row['start_date']))}, `
        + `${strValue(utils.formatDate(row['end_date']))})`;

    nextId++;
  });

  let insertStatement = insert + toBeinserted;
  return [insertStatement, nextId];
}

function prepareRelationshipTypeInsert(rows, nextId) {
  let insert = 'INSERT INTO relationship_type (relationship_type_id, a_is_to_b, '
        + 'b_is_to_a, preferred, weight, description, creator, date_created, '
        + 'uuid, retired, retired_by, date_retired, retire_reason) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
    toBeinserted += `(${nextId}, ${strValue(row['a_is_to_b'])}, `
        + `${strValue(row['b_is_to_a'])}, ${row['preferred']}, ${row['weight']}, `
        + `${strValue(row['description'])}, ${userMap.get(row['creator'])}, `
        + `${strValue(utils.formatDate(row['date_created']))}, `
        + `${uuid(row['uuid'])}, ${row['retired']}, ${retiredBy}, `
        + `${strValue(utils.formatDate(row['date_retired']))}, `
        + `${strValue(row['retire_reason'])})`;

    //Update the map
    beehive.relationshipTypeMap.set(row['relationship_type_id'], nextId);
    nextId++;
  });

  let insertStatement = insert + toBeinserted;
  return [insertStatement, nextId];
}

function preparePersonAttributeTypeInsert(rows, nextId) {
  let insert = 'INSERT INTO person_attribute_type(person_attribute_type_id, '
        + 'name, description, format, foreign_key, searchable, creator, '
        + 'date_created, changed_by, date_changed, retired, retired_by, '
        + 'date_retired, retire_reason, edit_privilege, uuid, sort_weight) '
        + 'VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let retireBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
    let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

    toBeinserted += `(${nextId}, ${strValue(row['name'])}, `
        + `${strValue(row['description'])}, ${strValue(row['format'])}, `
        + `${row['foreign_key']}, ${row['searchable']}, ${userMap.get(row['creator'])}, `
        + `${strValue(utils.formatDate(row['date_created']))}, ${changedBy}, `
        + `${strValue(utils.formatDate(row['date_changed']))}, ${row['retired']}, `
        + `${retiredBy}, ${strValue(utils.formatDate(row['date_retired']))}, `
        + `${strValue(row['retire_reason'])}, ${strValue(row['edit_privilege'])}, `
        + `${uuid(row['uuid'])}, ${row['sort_weight']})`;

    //Update the map
    beehive.personAttributeTypeMap.set(row['person_attribute_type_id'], nextId);
    nextId++;
  });

  let insertStatement = insert + toBeinserted;
  return [insertStatement, nextId];
}

function preparePersonAttributeInsert(rows, nextId) {
    let insert = 'INSERT INTO person_attribute(person_attribute_id, person_id, '
          + 'value, person_attribute_type_id, creator, date_created, changed_by, '
          + 'date_changed, voided, voided_by, date_voided, void_reason, uuid) '
          + 'VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
      if(toBeinserted.length > 1) {
        toBeinserted += ',';
      }
      let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
      let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

      toBeinserted += `(${nextId}, ${personMap.get(row['person_id'])}, `
          + `${strValue(row['value'])}, `
          + `${personAttributeTypeMap.get(row['person_attribute_type_id'])}, `
          + `${userMap.get(row['creator'])}, `
          + `${strValue(utils.formatDate(row['date_created']))}, `
          + `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, `
          + `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, `
          + `${strValue(row['void_reason'])}, ${uuid(row['uuid'])})`

      nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

function prepareRelationshipInsert(rows, nextId) {
  let insert = 'INSERT INTO relationship(relationship_id, person_a, relationship, '
        + 'person_b, creator, date_created, voided, voided_by, date_voided, '
        + 'void_reason, uuid, date_changed, changed_by, start_date, end_date)'
        + ' VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
    let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

    toBeinserted += `(${nextId}, ${personMap.get(row['person_a'])}, `
        + `${relationshipTypeMap.get(row['relationship'])}, `
        + `${personMap.get(row['person_b'])}, ${userMap.get(row['creator'])}, `
        + `${strValue(utils.formatDate(row['date_created']))}, `
        + `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, `
        + `${strValue(row['void_reason'])}, ${uuid(row['uuid'])}, `
        + `${strValue(utils.formatDate(row['date_changed']))}, `
        + `${changedBy}, ${strValue(utils.formatDate(row['start_date']))}, `
        + `${strValue(utils.formatDate(row['end_date']))})`;

    nextId++;
  });

  let insertStatement = insert + toBeinserted;
  return [insertStatement, nextId];
}

function prepareUserInsert(rows, nextUserId) {
  let insert = 'INSERT INTO users(user_id, system_id, username, password, salt,'
              + 'secret_question, secret_answer, creator, date_created, '
              + 'changed_by, date_changed, person_id, retired, retired_by, '
              + 'date_retired, retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }
      beehive.userMap.set(row['user_id'], nextUserId);
      toBeinserted += `(${nextUserId}, '${row['system_id']}', ${strValue(row['username'])},`
          + `'${row['password']}', '${row['salt']}', ${strValue(row['secret_question'])}, `
          + `${strValue(row['secret_answer'])}, ${userMap.get(row['creator'])}, `
          + `${strValue(utils.formatDate(row['date_created']))}, `
          + `${row['changed_by']}, `
          + `${strValue(utils.formatDate(row['date_changed']))}, `
          + `${personMap.get(row['person_id'])}, ${row['retired']}, `
          + `${row['retired_by']}, `
          + `${strValue(utils.formatDate(row['date_retired']))}, `
          + `${strValue(row['retire_reason'])}, ${uuid(row['uuid'])})`;

      nextUserId++;
  });

  let query = insert + toBeinserted;

  return [query, nextUserId];
}

function prepareRoleInsert(rows) {
  let insert = 'INSERT INTO role(role, description, uuid) VALUES ';
  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    toBeinserted += `(${strValue(row['role'])},`
    + `${strValue(row['description'])}, `
    + `${uuid(row['uuid'])})`;
  });
  return insert + toBeinserted;
}

function preparePrivilegeInsert(rows) {
  let insert = 'INSERT INTO privilege(privilege, description, uuid) VALUES ';
  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    toBeinserted += `(${strValue(row['privilege'])},`
    + `${strValue(row['description'])}, `
    + `${uuid(row['uuid'])})`;
  });
  return insert + toBeinserted;
}

function prepareRolePrivilegeInsert(rows) {
  let insert = 'INSERT IGNORE INTO role_privilege(role, privilege) VALUES ';
  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    toBeinserted += `(${strValue(row['role'])}, ${strValue(row['privilege'])})`;
  });
  return insert + toBeinserted;
}

function prepareRoleRoleInsert(rows) {
  let insert = 'INSERT IGNORE INTO role_role(parent_role, child_role) VALUES ';
  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    toBeinserted += `(${strValue(row['parent_role'])},`
                    + `${strValue(row['child_role'])})`;
  });
  return insert + toBeinserted;
}

function prepareUserRoleInsert(rows) {
  let insert = 'INSERT IGNORE INTO user_role(user_id, role) VALUES ';
  let toBeinserted = '';
  rows.forEach(row => {
    if(toBeinserted.length > 1) {
      toBeinserted += ',';
    }
    let userId = beehive.userMap.get(row['user_id']);
    if(userId) {
      toBeinserted += `(${userId}, ${strValue(row['role'])})`;
    }
  });
  return insert + toBeinserted;
}

async function consolidateRolesAndPrivileges(srcConn, destConn) {
  let __addRolesNotAlreadyInDestination = async function () {
      let roleQuery = 'SELECT * FROM role';
      let [sRoles] = await srcConn.query(roleQuery);
      let [dRoles] = await destConn.query(roleQuery);

      let rolesToAdd = sRoles.filter(sRole => {
        return dRoles.every(dRole => {
          return sRole.role !== dRole.role;
        });
      });
      if(rolesToAdd.length > 0) {
        // console.log('Adding Roles: ', rolesToAdd);
        let insertStmt = prepareRoleInsert(rolesToAdd);
        let [result] = await destConn.query(insertStmt);
        return result.affectedRows;
      }
      return 0;
  };

  let __addPrivilegesNotAlreadyInDestination = async function () {
    let query = 'SELECT * FROM privilege';
    let [sPrivs] = await srcConn.query(query);
    let [dPrivs] = await destConn.query(query);

    let privToAdd = sPrivs.filter(sPriv => {
      return dPrivs.every(dPriv => {
        return sPriv.privilege !== dPriv.privilege;
      });
    });
    if(privToAdd.length > 0) {
      // console.log('Adding Privileges: ', privToAdd);
      let insertStmt = preparePrivilegeInsert(privToAdd);
      let [result] = await destConn.query(insertStmt);
      return result.affectedRows;
    }
    return 0;
  };

  await __addPrivilegesNotAlreadyInDestination();
  await __addRolesNotAlreadyInDestination();

  //Insert role_privileges (insert ignore)
  let [rps] = await srcConn.query('SELECT * FROM role_privilege');
  if(rps.length > 0) {
    let stmt = prepareRolePrivilegeInsert(rps);
    let [result] = await destConn.query(stmt);
  }

  //Do the same sh*t for role_role
  let [rrs] = await srcConn.query('SELECT * FROM role_role');
  if(rrs.length>0) {
    stmt = prepareRoleRoleInsert(rrs);
    [result] = await destConn.query(stmt);
  }
}

async function consolidatePersonAttributeTypes(srcConn, destConn) {
  let query = 'SELECT * FROM person_attribute_type';
  let [sAttributeTypes] = await srcConn.query(query);
  let [dAttributeTypes] = await destConn.query(query);

  let toAdd = [];
  sAttributeTypes.forEach(sAttributeType => {
    let match = dAttributeTypes.find(dAttributeType => {
      return sAttributeType['name'] === dAttributeType['name'];
    });
    if(match !== undefined) {
      beehive.personAttributeTypeMap.set(sAttributeType['person_attribute_type_id'],
        match['person_attribute_type_id']);
    }
    else {
      toAdd.push(sAttributeType);
    }
  });
  if(toAdd.length > 0) {
    let nextId = await utils.getNextAutoIncrementId(destConn, 'person_attribute_type');

    let [stmt] = preparePersonAttributeTypeInsert(toAdd, nextId);
    let [result] = await destConn.query(stmt);
  }
}

async function consolidateRelationshipTypes(srcConn, destConn) {
  let query = 'SELECT * FROM relationship_type';
  let [sRelshipTypes] = await srcConn.query(query);
  let [dRelshipTypes] = await destConn.query(query);

  let toAdd = [];
  sRelshipTypes.forEach(sRelshipType => {
    let match = dRelshipTypes.find(dRelshipType => {
      return (sRelshipType['a_is_to_b'] === dRelshipType['a_is_to_b']
                  && sRelshipType['b_is_to_a'] === dRelshipType['b_is_to_a']);
    });
    if(match !== undefined) {
      beehive.relationshipTypeMap.set(sRelshipType['relationship_type_id'],
        match['relationship_type_id']);
    }
    else {
      toAdd.push(sRelshipType);
    }
  });
  if(toAdd.length > 0) {
    let nextRelationshipTypeId =
      await utils.getNextAutoIncrementId(destConn, 'relationship_type');

    let [stmt] = prepareRelationshipTypeInsert(toAdd, nextRelationshipTypeId);
    let [result] = await destConn.query(stmt);
  }
}

async function moveRelationships(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'relationship',
                              'relationship_id',prepareRelationshipInsert);
}

async function movePersonAddresses(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'person_address',
                          'person_address_id',preparePersonAddressInsert);
}

async function movePersonAttributes(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'person_attribute',
                          'person_attribute_id', preparePersonAttributeInsert);
}

async function updateMovedUsersRoles(srcConn, destConn) {
    let query = 'SELECT * FROM user_role WHERE user_id NOT IN (1,2)';
    let [rows] = await srcConn.query(query);
    if(rows.length > 0 ) {
      let insert = prepareUserRoleInsert(rows);
      let [result] = await destConn.query(insert);
    }
}

async function getUsersCount(connection, condition) {
    let countQuery = 'SELECT count(*) as users_count FROM users';

    if(condition) {
      countQuery += ' WHERE ' + condition;
    }

    try {
        let [results] = await connection.query(countQuery);
        return results[0]['users_count'];
    } catch (ex) {
        console.error('Error while fetching users count', ex);
        throw ex;
    }
}

async function getPersonsCount(connection, condition) {
    let personCountQuery = 'SELECT COUNT(*) as person_count from person';

    if (condition) {
        personCountQuery += ' WHERE ' + condition;
    }
    try {
        let [results, metadata] = await connection.query(personCountQuery);
        return results[0]['person_count'];
    } catch (ex) {
        console.error('Error while fetching number of records in person table');
        throw ex;
    }
}

async function createUserTree(connection, rootUserId, tree) {
    let tempTree = null;
    if (tree === undefined || tree === null) {
        tempTree = {
            userId: rootUserId,
            children: []
        };
    } else {
      tempTree = tree;
    }
    try {
        let childrenQuery = 'SELECT user_id from users WHERE creator = ' + rootUserId;
        let [rows, fields] = await connection.query(childrenQuery);
        if (rows.length > 0) {
            rows.forEach(row => {
              if(row['user_id'] !== rootUserId) {
                tempTree.children.push({
                    userId: row['user_id'],
                    children: []
                });
              }
            });

            // Build tree for each child.
            tempTree.children.forEach(child => {
                createUserTree(connection, child.userId, child);
            });
        }
        return tempTree;
    } catch (ex) {
        console.error('Error occured while building user tree', ex);
    }
}

async function movePersonNamesforMovedPersons(srcConn, destConn) {
    let fetchQuery = 'SELECT * FROM person_name order by person_name_id LIMIT ';
    let startingRecord = 0;
    let dynamicQuery = fetchQuery + `${startingRecord}, ${BATCH_SIZE}`;
    let [r, f] = await srcConn.query(dynamicQuery);
    let nextPersonNameId = -1;
    if(r.length>0) {
      nextPersonNameId = await utils.getNextAutoIncrementId(destConn, 'person_name');
    }

    let moved = 0;
    while(Array.isArray(r) && r.length>0) {
      let [insertStmt, nextId] = preparePersonNameInsert(r, nextPersonNameId);
      let [result, meta] = await destConn.query(insertStmt);
      moved += result.affectedRows;
      nextPersonNameId = nextId;

      startingRecord += BATCH_SIZE;
      dynamicQuery = fetchQuery + `${startingRecord}, ${BATCH_SIZE}`;
      [r, f] = await srcConn.query(dynamicQuery);
    }
    return moved;
}

async function movePersons(srcConn, destConn, srcUserId) {
    try {
        // Get next person id in the destination
        let nextPersonId = await utils.getNextAutoIncrementId(destConn, 'person');
        let personsToMoveCount = await getPersonsCount(srcConn, 'creator=' + srcUserId);

        // Get all person created by srcUserId in SRC database
        let startingRecord = 0;
        let personFetchQuery = `SELECT * FROM person WHERE creator = ${srcUserId}`
                              + ` order by date_created limit `;
        let temp = personsToMoveCount;
        let moved = 0;
        while (temp % BATCH_SIZE > 0) {
            let query = personFetchQuery;
            if (Math.floor(temp / BATCH_SIZE) > 0) {
                moved += BATCH_SIZE;
                query += startingRecord + ', ' + BATCH_SIZE;
                temp -= BATCH_SIZE;
            } else {
                moved += temp;
                query += startingRecord + ', ' + temp;
                temp = 0;
            }
            startingRecord += BATCH_SIZE;
            // console.log('fetch query', query);
            let [r, f] = await srcConn.query(query);
            let [q, nextId] = preparePersonInsert(r, nextPersonId);

            // Insert person records into destination machine.
            // console.log('Running query:', q);
            await destConn.query(q);
            nextPersonId = nextId;
        }
        return moved;
    } catch (ex) {
        console.error('An error occured while moving persons', ex);
        throw ex;
    }
}

async function moveUsers(srcConn, destConn, creatorId) {
  try {
    let condition = `creator=${creatorId} and system_id not in ('daemon','admin')`;
    let nextUserId = await utils.getNextAutoIncrementId(destConn, 'users');
    let usersToMoveCount = await getUsersCount(srcConn, condition);

    let startingRecord = 0;
    let userFetchQuery = 'SELECT * FROM users WHERE ' + condition
                        + ' order by date_changed, date_created LIMIT ';

    let temp = usersToMoveCount;
    let moved = 0
    while (temp % BATCH_SIZE > 0) {
        let query = userFetchQuery;
        if (Math.floor(temp / BATCH_SIZE) > 0) {
            moved += BATCH_SIZE;
            query += startingRecord + ', ' + BATCH_SIZE;
            temp -= BATCH_SIZE;
        } else {
            moved += temp;
            query += startingRecord + ', ' + temp;
            temp = 0;
        }
        startingRecord += BATCH_SIZE;
        let [records, fields] = await srcConn.query(query);
        let [insertStmt, nextId] = prepareUserInsert(records, nextUserId);

        // Insert data into destination
        await destConn.query(insertStmt);
        nextUserId = nextId;
    }
    return moved;
  } catch (ex) {
      console.error('An error occured while moving users', ex);
      throw ex;
  }
}

async function traverseUserTree(tree, srcConn,destConn) {
  if(!tree || tree.userId === undefined) {
    throw new Error('Error! Incompatible tree passed', tree);
  }
  try {
    let movedPersons = await movePersons(srcConn, destConn, tree.userId);
    let movedUsers = await moveUsers(srcConn, destConn, tree.userId);
    // For each child do the same.
    if(tree.children && tree.children.length>0) {
      for(let i=0; i < tree.children.length; i++) {
        let childMoved = await traverseUserTree(tree.children[i], srcConn, destConn);
        movedPersons += childMoved.movedPersonsCount;
        movedUsers += childMoved.movedUsersCount;
      }
    }

    return {
      movedPersonsCount: movedPersons,
      movedUsersCount: movedUsers
    };
  }
  catch(ex) {
    console.error('An error occured while traversing tree ', tree, ex);
    throw ex;
  }
}

async function mergeAlgorithm() {
    if(config.generateNewUuids === null || config.generateNewUuids === undefined) {
      throw new Error('Please specify how you want UUIDs to be handled by '
        + 'specifying "generateNewUuids" config option as true/false in '
        + 'config.json file');
    }
    let srcConn = null;
    let destConn = null;
    try {
        srcConn = await connection(config.source);
        destConn = await connection(config.destination);

        console.log('Fetching users count from source & destination...');
        const srcUsersCount = await getUsersCount(srcConn);
        const initialDestUsersCount = await getUsersCount(destConn);

        const srcPersonCount = await getPersonsCount(srcConn);
        const initialDestPersonCount = await getPersonsCount(destConn);

        console.log(`${logTime()}: Starting to move persons & users...`);
        console.log(`Number of persons in source db: ${srcPersonCount}`);
        console.log(`Number of users in source db: ${srcUsersCount}`);
        console.log(`Initial numnber of persons in destination: ${initialDestPersonCount}`);
        console.log(`Initial numnber of users in destination: ${initialDestUsersCount}`);

        // Get source's admin user. (This is usually user with user_id=1, user0)
        let srcAdminUserQuery = `SELECT * FROM users where user_id=1 or
        system_id='admin' order by user_id`;
        let [rows, fields] = await srcConn.query(srcAdminUserQuery);
        // console.log(rows);
        let srcAdminUserId = 1;
        if (rows.every(row => {
                return row['user_id'] ==! 1;
            })) {
            let r = rows.find(row => {
                return row['system_id'] === 'admin';
            });

            srcAdminUserId = r['user_id'];
        }

        //Update the user map with user0's mappings.
        beehive.userMap.set(srcAdminUserId, 1);

        // Create the user tree.
        let tree = await createUserTree(srcConn, srcAdminUserId);
        console.log('tree:', tree);

        // STEP1.1 Traverse user tree performing the following for each user
        let count = null;
        try {
          await destConn.query('START TRANSACTION');
          count = await traverseUserTree(tree, srcConn, destConn);
          await destConn.query('COMMIT');
        }
        catch(dbEx) {
          await destConn.query('ROLLBACK');
          throw dbEx;
        }

        const finalDestUserCount = await getUsersCount(destConn);
        const finalDestPersonCount = await getPersonsCount(destConn);

        //Do some crude math verifications.
        let expectedFinalDestUserCount = initialDestUsersCount + count.movedUsersCount;
        let expectedFinalDestPersonCount = initialDestPersonCount + count.movedPersonsCount;

        if(expectedFinalDestPersonCount === finalDestPersonCount &&
                      expectedFinalDestUserCount === finalDestUserCount) {
            console.log(`${logTime()}: Hooraa! Persons & Users Moved successfully!`);
            console.log(`${count.movedPersonsCount} persons moved and new destination total is ${finalDestPersonCount}`);
            console.log(`${count.movedUsersCount} users moved and new destination total is ${finalDestUserCount}`);
            console.log('Moving person names...');
            // TODO: Establish number of names in dest in order to compare at the end.
            try {
              await destConn.query('START TRANSACTION');
              count = await movePersonNamesforMovedPersons(srcConn, destConn);
              await destConn.query('COMMIT');

              console.log(`${count} names moved`);
              console.log(`Consolidating roles & privileges`);
              await destConn.query('START TRANSACTION');
              await consolidateRolesAndPrivileges(srcConn, destConn);
              await updateMovedUsersRoles(srcConn, destConn);
              await destConn.query('COMMIT');
              console.log('Consolidation successfully...');

              console.log('Upate moved person relationships');
              await destConn.query('START TRANSACTION');
              await consolidateRelationshipTypes(srcConn, destConn);
              await moveRelationships(srcConn, destConn);
              await movePersonAddresses(srcConn, destConn);
              await consolidatePersonAttributeTypes(srcConn, destConn);
              await movePersonAttributes(srcConn, destConn);
              await destConn.query('COMMIT');
              console.log('Relationships moved successfully...');
            }
            catch(dbEx) {
              await destConn.query('ROLLBACK');
              throw dbEx;
            }
        }
        else {
          console.error('Expected & actual numbers do not match!!');
          console.error('Too bad we are in deep sh*t!, We have a problem, terminating ...');
          process.exit(1);
        }
    } catch (ex) {
        console.error(ex);
    } finally {
        if (srcConn) srcConn.end();
        if (destConn) destConn.end();
    }
}

mergeAlgorithm();
