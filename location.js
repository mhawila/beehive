const utils = require('./utils');
const logLog = utils.logLog;
const logError = utils.logError;
const logTime = utils.logTime;
const strValue = utils.stringValue;
const getCount = utils.getCount;
const moveAllTableRecords = utils.moveAllTableRecords;

let beehive = global.beehive;
let notYetUpdatedWithParentLocations = new Map();

function prepareLocationInsert(rows, nextId) {
    let insert = 'INSERT INTO location(location_id, name, description, address1, ' +
        'address2, city_village, state_province, postal_code, country, latitude, ' +
        'longitude, creator, date_created, county_district, address3, address6, ' +
        'address5, address4, retired, retired_by, date_retired, retire_reason, ' +
        'parent_location, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        let parentLocation = beehive.locationMap.get(row['parent_location']);
        if(parentLocation === undefined) {
            parentLocation = null;
            if(row['parent_location'] !== null) {
                notYetUpdatedWithParentLocations.set(nextId, row['parent_location']);
            }
        }
        beehive.locationMap.set(row['location_id'], nextId);

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ${strValue(row['address1'])}, ` +
            `${strValue(row['address2'])}, ${strValue(row['city_village'])}, ` +
            `${strValue(row['state_province'])}, ${strValue(row['postal_code'])}, ` +
            `${strValue(row['country'])}, ${strValue(row['latitude'])}, ` +
            `${strValue(row['longitude'])}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${strValue(row['county_district'])}, ${strValue(row['address3'])}, ` +
            `${strValue(row['address6'])}, ${strValue(row['address5'])}, ` +
            `${strValue(row['address4'])}, ${row['retired']}, ${retiredBy}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${parentLocation}, ` +
            `${utils.uuid(row['uuid'])})`;

        nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

async function updateParentForLocations(connection, idMap) {
    if(idMap.size > 0) {
        let update = 'INSERT INTO location(location_id, parent_location) VALUES ';
        let lastPart = ' ON DUPLICATE KEY UPDATE parent_location = VALUES(parent_location)';

        let values = '';
        idMap.forEach((locationId, srcParentId) => {
            values += `(${locationId}, ${beehive.locationMap.get(srcParentId)})`;
        });

        let query = update + values + lastPart;
        utils.logDebug('Location parents update query:', query);
        await connection.query(query);
    }
    return idMap.size;
}

async function consolidateLocations(srcConn, destConn) {
    let query = 'SELECT * FROM location order by date_created';
    let [srcLocs] = await srcConn.query(query);
    let [destLocs] = await destConn.query(query);
    let [sql] = [null];

    try {
        let missingInDest = [];
        srcLocs.forEach(srcLoc => {
            let match = destLocs.find(destLoc => {
                return srcLoc['name'] === destLoc['name'];
            });

            if (match !== undefined && match !== null) {
                beehive.locationMap.set(srcLoc['location_id'], match['location_id']);
            } else {
                missingInDest.push(srcLoc);
            }
        });

        if (missingInDest.length > 0) {
            let nextLocationId = await utils.getNextAutoIncrementId(destConn, 'location');

            [sql] = prepareLocationInsert(missingInDest, nextLocationId);
            utils.logDebug('Location insert statement:\n', utils.shortenInsert(sql));
            let [result] = await destConn.query(sql);

            await updateParentForLocations(destConn, notYetUpdatedWithParentLocations);
            
            return result.affectedRows;
        }
        return 0;
    }
    catch(ex) {
        logError('Error while consolidating locations');
        if(sql) {
            logError('Statement during error:');
            logError(sql);
        }
        throw ex;
    }
}

module.exports = consolidateLocations;
