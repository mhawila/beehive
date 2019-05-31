(function() {
    global.beehive = {};
    global.startingStep = { 'atomic_step': 'pre-obs', 'passed': 0 };
    global.excludedPersonIds = [];
    global.excludedUsersIds = [];
    const utils = require('./utils');
    const stringValue = utils.stringValue;
    const ID_MAP_TABLE_PREFIX = 'beehive_merge_idmap_';

    // personMap also represents patientMap because person & patient
    // are one to one
    const BEEHIVE_MAPS_NAMES = {
        person: 'personMap',
        person_attribute_type: 'personAttributeTypeMap',
        relationship_type: 'relationshipTypeMap',
        users: 'userMap',
        patient_identifier_type: 'identifierTypeMap',
        location: 'locationMap',
        encounter: 'encounterMap',
        encounter_role: 'encounterRoleMap',
        encounter_type: 'encounterTypeMap',
        provider_attribute_type: 'providerAttributeTypeMap',
        provider: 'providerMap',
        visit_type: 'visitTypeMap',
        visit: 'visitMap',
        obs: 'obsMap'
    };

    Object.entries(BEEHIVE_MAPS_NAMES).forEach(([table, mapName]) => {
        global.beehive[mapName] = new Map();
    });

    //Add admin person_id mapping it to 1
    global.beehive.personMap.set(1,1);

    async function _sourceAlreadyExists(connection, source) {
        let query = 'SELECT source FROM beehive_merge_source where source = ' +
            `'${source}'`;
        [result] = await connection.query(query);
        if (result.length > 0) {
            return true;
        }
        return false;
    }

    /**
     * Prepares the destination for the new source. If persist flag is set to true, it means that the software
     * should persist progress changes to the database to allow it to proceed from where it stopped successfully
     * in cases where an error is encountered before the process is finished.
     */
    async function prepareForNewSource(srcConn, destConn, config) {
        let source = config.source.location;
        global.beehive['idMapTable'] = `${ID_MAP_TABLE_PREFIX}${source}`;

        let check = `SHOW TABLES LIKE 'beehive_merge_source'`;
        let [result] = await destConn.query(check);
        if (result.length === 0) {
            //Not created yet.
            await _createSourceTable(destConn);
        } else {
            if (!global.dryRun) {
                await _createProgressTables(destConn, source);
                // check if source already exists.
                let sourceExists = await _sourceAlreadyExists(destConn, source);
                if (!sourceExists) {
                    // Initial run
                    await _insertSource(destConn, source);
                } else {
                    // Second or more run
                    // TODO: Populate the maps from persisted tables
                    let finalStep = await _findPreviousRunFinalStep(destConn, source);
                    if(finalStep['atomic_step'] === 'post-obs' && finalStep['passed']) {
                        let error = `Location ${source} already processed`;
                        throw new Error(error);
                    } else {
                        global.startingStep = finalStep;
                        await _populateMapsFromPreviousRuns(destConn, source);
                    }
                }
            }
        }
        // prepare the excluded person_ids
        await _usersAndAssociatedPersonsToExclude(srcConn, destConn);
    }

    async function _insertSource(connection, source) {
        let s = `insert into beehive_merge_source values(${stringValue(source)})`;
        utils.logDebug(s);
        let [r] = await connection.query(s);
        return r.affectedRows;
    }

    async function _createSourceTable(connection) {
        let sourceTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_source(' +
            'source VARCHAR(50) PRIMARY KEY' +
            ')';

        utils.logDebug(sourceTable);
        await connection.query(sourceTable);
    }

    /**
     * Find the final step on which the previos run stopped at for a given source.
     * Three steps so (pre-obs, obs, post-obs)
     */
    async function _findPreviousRunFinalStep(connection, source) {
        let query = `SELECT * FROM beehive_merge_progress WHERE source = ${stringValue(source)} ` +
            'ORDER BY time_finished DESC LIMIT 1';
        let [result] = await connection.query(query);

        if(result.length === 0) return {
            'atomic_step': 'pre-obs',
            'passed': 0,
        };

        return result[0];
    }

    async function _createProgressTables(connection, source) {
        let progressTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_progress(' +
            'id INT(11) AUTO_INCREMENT PRIMARY KEY,' +
            'source VARCHAR(50) NOT NULL,' +
            'atomic_step VARCHAR(50) NOT NULL,' +
            'passed INT(11),' +
            'time_finished TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP' +
            ')';

        let idMapTable = `CREATE TABLE IF NOT EXISTS ${ID_MAP_TABLE_PREFIX}${source}(` +
            'table_name VARCHAR(50) NOT NULL, ' +
            'source_id INT(11) NOT NULL, ' +
            'destination_id INT(11) NOT NULL, ' +
            'CONSTRAINT unique_map_id_mapping_per_table UNIQUE(table_name, source))';

        let tables = [
            progressTable,
            idMapTable
        ];

        //Create these tables.
        for (let i = 0; i < tables.length; i++) {
            utils.logDebug(tables[i]);
            await connection.query(tables[i]);
        };
    }

    async function _usersAndAssociatedPersonsToExclude(srcConn, destConn) {
        let exclude = `SELECT * from users WHERE system_id IN ('daemon', 'admin')`;
        let [results] = await srcConn.query(exclude);
        global.excludedPersonIds = results.map(result => result['person_id']);
        global.excludedUsersIds = results.map(result => result['user_id']);

        let q = `SELECT * FROM users where system_id NOT IN ('admin', 'daemon')`;
        let [srcUsers] = await srcConn.query(q);
        let [destUsers] = await destConn.query(q);

        srcUsers.forEach(su => {
            let match = destUsers.find(du => {
                return ((su['system_id'] === du['system_id'] &&
                            su['username'] === du['username'])
                            || su['uuid'] === du['uuid']);
            });

            if(match) {
                global.excludedUsersIds.push(su['user_id']);
                global.excludedPersonIds.push(su['person_id']);
                global.beehive.userMap.set(su['user_id'], match['user_id']);
                global.beehive.personMap.set(su['person_id'], match['person_id']);
            }
        });
    }

    async function _populateMapsFromPreviousRuns(connection, source) {
        let entries = Object.entries(ID_MAP_TABLE_PREFIX);
        for(let [tableName, mapName] of Object.entries(ID_MAP_TABLE_PREFIX)) {
            await __populateAMapFromDb(connection, source, tableName, mapName);
        }
    }

    async function __populateAMapFromDb(connection, source, tableName, mapName) {
        let query = `SELECT * FROM ${ID_MAP_TABLE_PREFIX}${source} WHERE table_name = ${stringValue(tableName)}`;`
        let [idMappings] = await connection.query(query);

        if(idMappings.length > 0) {
            idMappings.forEach(mapping => {
                global.beehive[mapName].set(mapping['source_id'], mapping['destination_id']);
            });
        }
    }

    module.exports = {
        prepare: prepareForNewSource,
    };
})();
