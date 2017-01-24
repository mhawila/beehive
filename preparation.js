(function() {
  global.beehive = {};   // will be exported

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

  async function prepareForNewSource(connection, source) {
    let check = 'SHOW TABLES LIKE beehive_merge_source';
    let [result] = await connection.query(check);
    if(result.length === 0) {
      //Not created yet.
      await _createTables(connection);
      await _insertSource(connection, source);
    }
    else {
      // check if source already exists.
      let query = 'SELECT source FROM beehive_merge_source where source = '
                  + `'${source}'`;
      [result] = await connection.query(query);
      if(result.length === 0) {
         await _insertSource(connection, source);
      }
      else {
        // TODO: Populate the maps.
      }
    }
  }

  _createMapTable(tableSuffix) {
    let stmt = 'CREATE TABLE IF NOT EXISTS beehive_merge_' + tableSuffix
              + '(source VARCHAR(50),'
              + 'src_id INT(11) NOT NULL,'
              + 'dest_id INT(11) NOT NULL,'
              + 'UNIQUE(source, src_id)'
              + ')';
  }

  async _insertSource(connection, source) {
    let s = `insert into beehive_merge_source values('${source}')`;
    let [r] = await connection.query(s);
    return r.affectedRows;
  }

  async function _createTables(connection) {
    let sourceTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_source('
          + 'source VARCHAR(50) UNIQUE'
          + ')';

    let progressTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_progress('
      + 'id INT(11) AUTO_INCREMENT PRIMARY KEY,'
      + 'source VARCHAR(50) NOT NULL,'
      + 'atomi_step VARCHAR(50) NOT NULL,'
      + 'passed TINYINT,'
      + 'time_finished TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
      + ')';

      let tables = [
        sourceTable,
        progressTable
      ];
      try {
        //Create these tables.
        for(let i=0; i < tables.length; i++) {
          await connection.query(tables[i]);
        };

        for(let i=0; i < beehiveMapNames; i++) {
          let suffix = beehiveMapNames[0].toLowerCase();
          await connection.query(_createMapTable(suffix));
        }
      }
      catch(ex) {
        console.error('Error during merge preparations');
        throw ex;
      }
  }

  module.exports = {
    prepare: prepareForNewSource,
  };
})();
