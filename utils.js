'use strict';
const moment = require('moment');
const mysql = require('mysql2');
const uuidGenerator = require('uuid/v1');
const config = require('./config');

if (config.batchSize === undefined) {
    config.batchSize = 500;
};

let updateATransactionStep = async function(connection, step, passed, movedRecords) {
    if(passed === undefined) passed = 1;
    let query = 'INSERT INTO beehive_merge_progress (source, atomic_step, passed';
    if(movedRecords !== undefined && movedRecords !== null) {
        query += ', moved_records) VALUES ' +
            `(${stringValue(config.source.location)}, ${stringValue(step)}, ${passed}, ${movedRecords})`;
    } else {
        query += `) VALUES (${stringValue(config.source.location)}, ${stringValue(step)}, ${passed})`;
    }

    await connection.query(query);
};

/**
 * Copy the IDs (such as person_id) maps between source database records with destination database corresponding
 * record to the database. These can later be loaded in a multi-transaction move in which case the move resumes
 * from where the system stopped processing (mainly due to some exception)
 * @param connection: Database connection where the map is stored (destination is used in practice)
 * @param idMap: Native JS Map The table ID mappings to be copied (e.g. personMap,obsMap)
 * @param table: String Name of the table corresponding to idMap to be copied.
 */
let copyIdMapToDb = async function(connection, idMap, table) {
    let temp = idMap.size;
    let copied = 0;
    let queryLogged = false;
    let mapEntries = idMap.entries();
    let insertPrefix = `INSERT INTO ${global.beehive['idMapTable']}(table_name, source_id, destination_id) VALUES `;
    let query = null;
    try {
        while (temp > 0) {
            let toBeinserted = '';
            let limit = -1;
            if (Math.floor(temp / config.batchSize) > 0) {
                limit = config.batchSize;
                temp -= config.batchSize;
            } else {
                limit = temp;
                temp = 0;
            }

            for(let x=0; x < limit; x++) {
                if (toBeinserted.length > 1) {
                    toBeinserted += ',';
                }
                let entry = mapEntries.next();
                if(entry.done) {
                    break;
                }
                toBeinserted += `(${stringValue(table)}, ${entry.value[0]}, ${entry.value[1]})`;
            }
            query = insertPrefix + toBeinserted;

            if (!queryLogged) {
                logDebug(`${global.beehive['idMapTable']} insert statement:\n`, shortenInsertStatement(query));
                queryLogged = true;
            }

            let [r] = await connection.query(query);
            copied += r.affectedRows;
        }
        return copied;
    }
    catch(ex) {
        logError(`An error occured when copying ID map for table ${table}`);
        if(query) {
            logError('Insert statement during error');
            logError(query);
        }
        throw ex;
    }
};

let getNextAutoIncrementId = async function(connection, table) {
    if (arguments.length < 2) {
        throw new Error('This utility function expects connection & table in that order');
    }

    let query = `SELECT AUTO_INCREMENT as next_auto FROM information_schema.tables
  WHERE table_name=? and table_schema=database()`;
    try {
        let [r, f] = await connection.execute(query, [table]);
        return r[0]['next_auto'];
    } catch (trouble) {
        console.error('An error occured while fetching next auto increment for table ' +
            table, ':\n', trouble);
        throw trouble;
    }
};

let getCount = async function(connection, table, condition) {
    let countQuery = `SELECT count(*) as table_count FROM ${table}`;
    if (condition) {
        countQuery += ' WHERE ' + condition;
    }

    let [results] = await connection.query(countQuery);
    return results[0]['table_count'];
}

let formatDate = function(d, format) {
    //some how undefined is parsed by moment!!!!
    if (d == undefined) return null;
    if (moment(d).isValid()) {
        return moment(d).format('YYYY-MM-DD HH:mm:ss');
    }
    return null;
};

let logTime = function() {
    return formatDate(Date.now());
}

let stringValue = function(value) {
    return mysql.escape(value);
}

function uuid(existing) {
    if (config.generateNewUuids) return `'${uuidGenerator()}'`;
    return `'${existing}'`;
}

/*
 * Utility function that consolidate table records for meta data tables such as
 * visit_type, program e.t.c
 * @param srcConn
 * @param destConn
 * @param table:String Name of table whose records are to be consolidated.
 * @param comparisonColumns: Stringn|Array Name of the column(s) to base
 *          comparisons between source & destination. For example, if you
 *          specify name for program table then two recorded with same name value
 *          are considered identical regardless of other field values.
 *          If array each element can either be a simple column name string or
 *          an object of the form below
 *           {
 *              name: 'column name',
 *              mapped: 'boolean',      // Whether column value is mapped or not
 *              mappedValueMap: Map     // if mapped the map value is found here.
 *          }
 * @param idColumn: String Name of the primary key field (id) to be stored in
 *          src => dest idMap for this table.
 * @param idMap: Map a map of src_table_id => dest_table_id for the table.
 * @param insertQueryPrepareFunction: function prepares the insert query
 * @return count of records added to destination. (or a promise that resolves to count)
 */
let consolidateTableRecords = async function(srcConn, destConn, table,
    comparisonColumns, idColumn, idMap, insertQueryPrepareFunction) {
    let query = `SELECT * FROM ${table}`;
    let [srcRecords] = await srcConn.query(query);
    let [destRecords] = await destConn.query(query);

    let missingInDest = [];
    srcRecords.forEach(srcRecord => {
        let match = destRecords.find(destRecord => {
            if(Array.isArray(comparisonColumns)) {
                let compareResult = true;
                comparisonColumns.forEach(col => {
                    if(typeof col === 'string') {
                        compareResult = compareResult && (
                            srcRecord[col] === destRecord[col]
                        );
                    }
                    else {
                        if(col.mapped) {
                            compareResult = compareResult && (
                                col.mappedValueMap.get(srcRecord[col.name]) === destRecord[col.name]
                            );
                        }
                        else {
                            compareResult = compareResult && (
                                srcRecord[col.name] === destRecord[col.name]
                            );
                        }
                    }
                })
                return compareResult;
            }
            else {
                return srcRecord[comparisonColumns] === destRecord[comparisonColumns];
            }
        });

        if (match !== undefined && match !== null) {
            idMap.set(srcRecord[idColumn],
                match[idColumn]);
        } else {
            missingInDest.push(srcRecord);
        }
    });

    if (missingInDest.length > 0) {
        let sql = null;
        try {
            let nextDestId = await getNextAutoIncrementId(destConn, table);

            [sql] = insertQueryPrepareFunction(missingInDest, nextDestId);
            logDebug(`${table} insert statement:\n ${shortenInsertStatement(sql)}`);
            let [result] = await destConn.query(sql);
            return result.affectedRows;
        }
        catch(ex) {
            logError(`An error occured while consolidating ${table} records`);
            if(sql) {
                logError('Insert statement during error');
                logError(sql);
            }
            throw ex;
        }
    }
    else {
        return 0;
    }
}

/**
 * Utility function that moves all table records in config.batchSize batches
 * @param srcConn
 * @param destConn
 * @param tableName:String Name of table whose records are to be moved.
 * @param orderColumn:String Name of the column to order records with.
 * @param insertQueryPrepareFunction: function prepares the insert query
 * @return count of records moved. (or a promise that resolves to count)
 */
let moveAllTableRecords = async function(srcConn, destConn, tableName, orderColumn,
    insertQueryPrepareFunction, condition) {
    // Get the count to be pushed
    let countToMove = await getCount(srcConn, tableName, condition);
    let nextAutoIncr = await getNextAutoIncrementId(destConn, tableName);

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
            [q, nextId] = insertQueryPrepareFunction.call(null, r, nextAutoIncr);

            if (!queryLogged) {
                logDebug(`${tableName} insert statement:\n`, shortenInsertStatement(q));
                queryLogged = true;
            }

            if(q) {
                [r] = await destConn.query(q);
                moved += r.affectedRows;
            }

            nextAutoIncr = nextId;
        }
        return moved;
    }
    catch(ex) {
        logError(`An error occured when moving ${tableName} records`);
        if(q) {
            logError('Select statement:', query);
            logError('Insert statement during error');
            logError(q);
        }
        throw ex;
    }
};

let logError = function(...args) {
    args.splice(0, 0, "\x1b[31m");
    console.error.apply(null, args);
}

let logOk = function(...args) {
    args.splice(0, 0, "\x1b[32m");
    console.log.apply(null, args);
}

let logDebug = function(...args) {
    args.splice(0, 0, "\x1b[33m");
    if (config.debug) {
        console.log.apply(null, args);
    }
}

let logInfo = function(...args) {
    args.splice(0, 0, "\x1b[37m");
    console.log.apply(null, args);
}

let shortenInsertStatement = function(statement) {
    let charcount = 700;
    if(statement === undefined || statement === null) return statement;
    let valuesIndex = statement.indexOf('VALUES');
    if(valuesIndex == -1)return statement
    if(statement.substring(valuesIndex).length <= charcount) return statement;

    let lastParenth = statement.lastIndexOf(')', valuesIndex + charcount);
    return statement.substring(0, lastParenth + 1) + '...';
}

async function personIdsToexclude(connection) {
    // Get the person associated with daemon user
    let exclude = `SELECT person_id from users WHERE system_id IN ('daemon', 'admin')`;
    let [ids] = await connection.query(exclude);
    return ids.map(id => id['person_id']);
}

/**
 * Return a string representation of time that is split time in hours, mins, sec and ms.
 * @param ms: Integer time elapsed in milliseconds.
 * @return string: a printable representation of passed milliseconds in equivalent hrs, mins, secs, and ms.
 */
const getSimpleTimeString = (ms) => {
    if(ms >= 1000) {
        let hours = 0;
        let mins = 0;
        let min_less60 = 0;
        let secs_less60 = 0;
        let secs = Math.floor(ms/1000);
        let ms_less1000 = ms % 1000;

        if(secs >= 60) {
            mins = Math.floor(secs/60);
            secs_less60 = secs % 60;

            if(mins >= 60) {
                hours = Math.floor(mins/60);
                min_less60 = mins % 60;
            }
        }

        let str = '';
        if(hours > 0) {
            str += `${hours} hour${ hours > 1 ? 's' : '' }`;

            if(min_less60 > 0) {
                str += `, ${min_less60} min${ min_less60 > 1 ? 's' : '' }`;
            }

            if(secs_less60 > 0) {
                str += `, ${secs_less60} sec${ secs_less60 > 1 ? 's' : '' }`;
            }
        } else if(mins > 0) {
            str += `${mins} min${ mins > 1 ? 's' : '' }`;
            if(secs_less60 > 0) {
                str += `, ${secs_less60} sec${ secs_less60 > 1 ? 's' : '' }`;
            }
        } else {
            str += `${secs} sec${ secs > 1 ? 's' : '' }`;
        }
        if(ms_less1000 > 0) {
            str += `, ${ms_less1000} ms`;
        }

        return str;
    } else {
        return `${ms} ms`;
    }
};

module.exports = {
    getSimpleTimeString: getSimpleTimeString,
    copyIdMapToDb: copyIdMapToDb,
    updateATransactionStep: updateATransactionStep,
    getNextAutoIncrementId: getNextAutoIncrementId,
    getCount: getCount,
    stringValue: stringValue,
    moveAllTableRecords: moveAllTableRecords,
    formatDate: formatDate,
    logTime: logTime,
    logOk: logOk,
    logError: logError,
    logDebug: logDebug,
    logInfo: logInfo,
    uuid: uuid,
    shortenInsert: shortenInsertStatement,
    consolidateRecords: consolidateTableRecords,
    personIdsToexclude: personIdsToexclude,
};
