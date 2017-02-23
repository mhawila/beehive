(function() {
    global.beehive = {};
    const utils = require('./utils');
    const stringValue = utils.stringValue;

    let beehiveMapNames = [
        'personMap',
        'personAttributeTypeMap',
        'relationshipTypeMap',
        'userMap',
        'identifierTypeMap',
        'locationMap',
        'encounterMap',
        'encounterRoleMap',
        'encounterTypeMap',
        'providerAttributeTypeMap',
        'providerMap',
        'visitTypeMap',
        'visitMap',
        'obsMap'
    ];

    beehiveMapNames.forEach(mapName => {
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

    async function prepareForNewSource(connection, config) {
        let source = config.source.location;
        let persist = config.persist || false;

        let check = `SHOW TABLES LIKE 'beehive_merge_source'`;
        let [result] = await connection.query(check);
        if (result.length === 0) {
            //Not created yet.
            await _createSourceTable(connection);
            if (persist) {
                await _createTables(connection);
            }
            // await _insertSource(connection, source);
        } else {
            // TODO: If we decide to do transaction in chunks this section will be relevant
            // check if source already exists.
            let sourceExists = await _sourceAlreadyExists(connection, source);
            if (persist) {
                if (!sourceExists) {
                    // Initial run
                    await _insertSource(connection, source);
                } else {
                    // Second or more run
                    // TODO: Populate the maps from persisted tables
                }
            }
            else {
                if(sourceExists){
                    let error = `Location ${source} already processed`;
                    throw new Error(error);
                }
            }
        }
    }

    function _createMapTable(tableSuffix) {
        let stmt = 'CREATE TABLE IF NOT EXISTS beehive_merge_' + tableSuffix +
            '(source VARCHAR(50),' +
            'src_id INT(11) NOT NULL,' +
            'dest_id INT(11) NOT NULL,' +
            'UNIQUE(source, src_id)' +
            ')';
        return stmt;
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

    async function _createTables(connection) {
        let progressTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_progress(' +
            'id INT(11) AUTO_INCREMENT PRIMARY KEY,' +
            'source VARCHAR(50) NOT NULL,' +
            'atomi_step VARCHAR(50) NOT NULL,' +
            'passed TINYINT,' +
            'time_finished TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP' +
            ')';

        let tables = [
            progressTable
        ];

        //Create these tables.
        for (let i = 0; i < tables.length; i++) {
            utils.logDebug(tables[i]);
            await connection.query(tables[i]);
        };

        for (let i = 0; i < beehiveMapNames.length; i++) {
            let suffix = beehiveMapNames[i].toLowerCase();
            let mapTable = _createMapTable(suffix);
            if (i === 0) utils.logDebug('MapTables Statement', mapTable);
            await connection.query(mapTable);
        }
    }

    module.exports = {
        prepare: prepareForNewSource,
        insertSource: _insertSource
    };
})();
