'use strict';
const moment = require('moment');
const connection = require('./connection').connection;
const utils = require('./utils');
const config = require('./config');
const _ = require('lodash');
const userMap = new Map();
const personMap = new Map();
const BATCH_SIZE = config.batchSize || 500;

async function getUsersCount(connection, condition) {
    let countQuery = 'SELECT count(*) as users_count FROM users';

    if(condition) {
      countQuery += ' WHERE ' + condition;
    }

    try {
        let [results, metadata] = await connection.query(countQuery);
        return results[0]['users_count'];
    } catch (ex) {
        console.error('Error while fetching users count', ex);
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
    }
}

function preparePersonInsert(rows, nextPersonId) {
    let insert = 'INSERT INTO person(person_id, gender, birthdate,' +
        'birthdate_estimated, dead, deathdate, cause_of_death, creator,' +
        'date_created, changed_by, date_changed, voided, voided_by,' +
        'date_voided, void_reason, uuid) VALUES ';

    let toBeinserted = '';
    _.each(rows, row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        personMap.set(row['person_id'], nextPersonId);
        toBeinserted += `(${nextPersonId}, ${row['gender']}, ${utils.formatDate(row['birthdate'])},` +
            `${row['birthdate_estimated']}, ${row['dead']},` +
            `${utils.formatDate(row['deathdate'])}, ${row['cause_of_death']},` +
            `${userMap.get(row['creator'])}, ${utils.formatDate(row['date_created'])},` +
            `${row['changed_by']}, ${utils.formatDate(row['date_changed'])},` +
            `${row['voided']}, ${row['voided_by']},` +
            `${utils.formatDate(row['date_voided'])},` +
            `${row['void_reason']}, ${row['uuid']})`;
        nextPersonId++;
    })

    let query = insert + toBeinserted;

    return [query, nextPersonId];
}

function prepareUserInsert(rows, nextUserId) {
  let insert = 'INSERT INTO users(user_id, system_id, username, password, salt,'
              + 'secret_question, secret_answer, creator, date_created, '
              + 'changed_by, date_changed, person_id, retired, retired_by, '
              + 'date_retired, retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  _.each(rows, row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }
      userMap.set(row['user_id'], nextUserId);
      toBeinserted += `(${nextUserId}, ${row['system_id']}, ${row['username']},`
          + `${row['password']}, ${row['salt']}, ${row[secret_question]}, `
          + `${row['secret_answer']}, ${userMap.get(row['creator'])}`,
          + `${utils.formatDate(row['date_created'])}, `
          + `${userMap.get(row['changed_by'])}, `
          + `${utils.formatDate(row['date_changed'])},`
          + `${personMap.get(row['person_id'])}, ${row['retired']}, `
          + `${userMap.get(row['retired_by'])}, `
          + `${utils.formatDate(row['date_retired'])},` +
          + `${row['retire_reason']}, ${row['uuid']})`;
      nextPersonId++;
  })

  let query = insert + toBeinserted;

  return [query, nextPersonId];
}

function movePersonNamesforMovedPersons() {
    //TODO
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
            _.each(rows, row => {
              if(row['user_id'] !== rootUserId) {
                tempTree.children.push({
                    userId: row['user_id'],
                    children: []
                });
              }
            });

            // Build tree for each child.
            _.each(tempTree.children, child => {
                createUserTree(connection, child.userId, child);
            });
        }
        return tempTree;
    } catch (ex) {
        console.error('Error occured while building user tree', ex);
    }
}

async function movePersons(srcConn, destConn, srcUserId) {
    try {
        // Get next person id in the destination
        let nextPersonId = await utils.getNextAutoIncrementId(destConn, 'person');
        let personsToMoveCount = await getPersonsCount(srcConn, 'creator=' + srcUserId);

        // Get all person created by user0 in SRC database
        let startingRecord = 0;
        let personFetchQuery = `SELECT * FROM person WHERE creator = ${srcUserId}`
                              + `order by date_created limit `;
        let temp = personsToMoveCount;
        while (temp % BATCH_SIZE > 0) {
            let query = personFetchQuery;
            if (temp / BATCH_SIZE > 0) {
                query += startingRecord + ', ' + BATCH_SIZE;
                temp -= BATCH_SIZE;
            } else {
                query += startingRecord + ', ' + temp;
                temp = 0;
            }
            startingRecord += BATCH_SIZE;
            let [r, f] = await srcConn.query(query);
            let [q, nextId] = preparePersonInsert(r, nextPersonId);
            // console.log(r);
            console.log('query ni:', q);
            console.log('id inayofuata ni:', nextId);
            // TODO: Insert person records into destination machine.
            nextPersonId = nextId;
        }
    } catch (ex) {
        console.error('An error occured while moving persons', ex);
    }
}

async function moveUsers(srcConn, destConn, creatorId) {
  try {
    let nextUserId = await utils.getNextAutoIncrementId(destConn, 'users');
    let usersToMoveCount = await getUsersCount(srcConn, 'creator=' + creatorId);

    let startingRecord = 0;
    let userFetchQuery = `SELECT * FROM users WHERE creator=${creatorId}`
                    + ` order by date_changed, date_created LIMIT `;

    let temp = usersToMoveCount;
    while (temp % BATCH_SIZE > 0) {
        let query = userFetchQuery;
        if (temp / BATCH_SIZE > 0) {
            query += startingRecord + ', ' + BATCH_SIZE;
            temp -= BATCH_SIZE;
        } else {
            query += startingRecord + ', ' + temp;
            temp = 0;
        }
        startingRecord += BATCH_SIZE;
        let [records, fields] = await srcConn.query(query);
        let [insertStmt, nextId] = prepareUserInsert(r, nextUserId);
        // console.log(r);
        console.log('query ni:', q);
        console.log('id inayofuata ni:', nextId);

        // TODO: Insert data into destination (using transactions)
        nextUserId = nextId;
    }
  } catch (ex) {
      console.error('An error occured while moving users', ex);
  }
}

async function traverseUserTree(tree, srcConn,destConn) {
  if(!tree || tree.userId === undefined) {
    throw new Error('Error! Incompatible tree passed', tree);
  }
  try {
    await movePersons(srcConn, destConn, tree.userId);
    await moveUsers(srcConn, destConn, tree.userId);
    //For each child do the same.
    if(tree.children && tree.children.length>0) {
      _.each(tree.children, child => {
        traverseUserTree(child, srcConn, destConn);
      })
    }
  }
  catch(ex) {
    console.error('An error occured while traversing tree ', tree, ex);
  }
}

async function mergeAlgorithm() {
    let srcConn = null;
    let destConn = null;
    try {
        srcConn = await connection(config.source);
        destConn = await connection(config.destination);

        console.log('Fetching users count from source & destination...');
        const srcUsersCount = await getUsersCount(srcConn);
        const initialDestUsersCount = await getUsersCount(destConn);
        console.log('Number of users in source db: ', srcUsersCount);
        console.log('Initial numnber of users in destination: ',
            initialDestUsersCount);

        const srcPersonCount = await getPersonsCount(srcConn);
        const initialDestPersonCount = await getPersonsCount(destConn);

        // Get source's admin user. (This is usually user with user_id=1, user0)
        let srcAdminUserQuery = `SELECT * FROM users where user_id=1 or
        system_id='admin' order by user_id`;
        let [rows, fields] = await srcConn.query(srcAdminUserQuery);
        // console.log(rows);
        let srcAdminUserId = 1;
        if (!_.some(rows, row => {
                return row['user_id'] === 1;
            })) {
            let r = _.find(rows, row => {
                return row['system_id'] === 'admin';
            });

            srcAdminUserId = r['user_id'];
        }

        //Update the user map with user0's mappings.
        userMap.set(srcAdminUserId, 1);

        // Create the user tree.
        let tree = await createUserTree(srcConn, srcAdminUserId);
        console.log('tree:', tree);
        // Traverse user tree performing the following for each user
        //await traverseUserTree(tree, srcConn, destConn);

    } catch (ex) {
        console.error('Something terrible happened here!', ex);
    } finally {
        if (srcConn) srcConn.end();
        if (destConn) destConn.end();
    }
}

mergeAlgorithm();
