const utils = require('./utils');
const logError = utils.logError;
const strValue = utils.stringValue;

let beehive = global.beehive;
let notYetUpdatedWithParentLocations = new Map();

function prepareLocationInsert(rows, nextId) {
    let insert = 'INSERT IGNORE INTO location(location_id, name, description, address1, ' +
        'address2, city_village, state_province, postal_code, country, latitude, ' +
        'longitude, creator, date_created, county_district, address3, address6, ' +
        'address5, address4, retired, retired_by, date_retired, retire_reason, ' +
        'parent_location, uuid, changed_by, date_changed';

    if(global.openmrsDataModelVersion === 2) {
        insert += ', address7, address8, address9, address10, ' +
        'address11, address12, address13, address14, address15) VALUES ';
    } else {
        insert += ') VALUES ';
    }
    
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
            `${utils.uuid(row['uuid'])}, ${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}`;

        if(global.openmrsDataModelVersion === 2) {
            toBeinserted +=  `, ${strValue(row['address7'])}, ${strValue(row['address8'])}, ${strValue(row['address9'])},` +
            `${strValue(row['address10'])}, ${strValue(row['address11'])}, ${strValue(row['address12'])},` +
            `${strValue(row['address13'])}, ${strValue(row['address14'])}, ${strValue(row['address15'])})`;
        } else {
            toBeinserted += ')';
        }
           
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
        idMap.forEach((srcParentId, locationId) => {
            if(values.length > 1) {
                values += ',';
            }
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
                return ((srcLoc['name'] === destLoc['name'] &&
                             srcLoc['county_district'] === destLoc['county_district'] &&
                             srcLoc['state_province'] === destLoc['state_province']) ||
                                srcLoc['uuid'] === destLoc['uuid']);
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
