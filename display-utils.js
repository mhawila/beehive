'use strict';
/**
 * prettyPrintRows print passed rows in tabular format.
 * @param rows: An array of array/object rows. if array of objects, then the
          objects consist of key=>value pairs.
 * @param colHeaders: An array/object of column headers. If the rows is an objects
          based array, then this parameter must be object of key=>value pairs, with
          keys being the keys for objects passed in the rows parameter.
          example: Array based rows & colHeaders params.
          headers = ['Shule', 'Wilaya', 'Mkoa'];
          rows = [
              ['kilosa', 'kilosa', 'morogoro'],
              ['Mazinyungu', 'kilosa', 'morogoro'],
              ['Sima', 'Bariadi', 'Simiyu'],
              ['Benjamin Mkapa', 'Ilala', 'Dar es salaam']
          ];
          example: Object based rows & colHeaders params
          rows = [ { item: 'sahani', value: 20},{ item: 'cup', value: 23}];
          headers = { item: 'Item', value: 'Value' };
 */
function prettyPrintRows(rows, colHeaders) {

    const __calculateColumnWidths = (rows, colHeaders) => {
        //for each col find the widest value.
        let columnWidths = [];
        if(Array.isArray(colHeaders)) {
            columnWidths.length = colHeaders.length;
            for(let i=0; i<colHeaders.length; i++) {
                columnWidths[i] = colHeaders[i].length;
            }
        }
        else {
            // Assume object
            let keys = Object.keys(colHeaders);
            columnWidths.length = keys.length;
            for(let i=0; i<keys.length; i++) {
                columnWidths[i] = String(colHeaders[keys[i]]).length;
            }
        }

        rows.forEach(row => {
            if(Array.isArray(row)) {
                row = row.map(value => String(value));
                for(let i=0; i<row.length; i++) {
                    if(row[i].length > columnWidths[i]) {
                        columnWidths[i] = row[i].length;
                    }
                }
            }
            else {
                // Object based. This will require the header to be object mapping
                // keys to header names.
                if(Array.isArray(colHeaders)) {
                    let message = `if passed rows are objects the passed ` +
                        `header columns should an object mapping the keys ` +
                        `to column display values`;
                    throw new Error(message);
                }
                let keys = Object.keys(colHeaders);
                for(let i=0; i<keys.length; i++) {
                    if(String(row[keys[i]]).length > columnWidths[i]) {
                        columnWidths[i] = String(row[keys[i]]).length;
                    }
                }
            }
        });
        return columnWidths.map(width => width + 2 );
    }

    const _printBorder = (columnWidths) => {
        for(let i=0; i < columnWidths.length; i++) {
            process.stdout.write('+');
            for(let j=1; j <= columnWidths[i]; j++) process.stdout.write('-');
        }
        process.stdout.write('+\n');
    }

    const __printValue = (value, colWidth) => {
        process.stdout.write('| ' + value);
        let rightPadLen = colWidth - String(value).length - 1;
        for(let j=1; j<=rightPadLen; j++) {
            process.stdout.write(' ');
        }
    }

    const _printLine = (line, columnWidths) => {
        if(Array.isArray(line)) {
            for(let i=0; i<line.length; i++) {
                __printValue(line[i], columnWidths[i]);
            }
        }
        else {
            let keys = Object.keys(colHeaders);
            for(let i=0; i<keys.length; i++) {
                __printValue(line[keys[i]], columnWidths[i]);
            }
        }
        process.stdout.write('|\n');
    }

    const columnWidths = __calculateColumnWidths(rows, colHeaders);
    _printBorder(columnWidths);

    if(colHeaders) {
        _printLine(colHeaders, columnWidths);
        _printBorder(columnWidths);
    }

    // Print values
    rows.forEach(row => {
        _printLine(row, columnWidths);
    });

    // Print bottom border line
    _printBorder(columnWidths);
}

module.exports = {
    prettyPrintRows: prettyPrintRows,
};

// (function test() {
//     let headers = ['Shule', 'Wilaya', 'Mkoa'];
//     let rows = [
//         ['kilosa', null, 'morogoro'],
//         ['Mazinyungu', 'kilosa', 'morogoro'],
//         ['Sima', 'Bariadi', 'Simiyu'],
//         ['Benjamin Mkapa', 'Ilala', 'Dar es salaam']
//     ];
//     prettyPrintRows(rows, headers);
//
//     headers = { item: 'Item', value: 'Value'};
//     rows = [ {item: 'Salsa', value: 34}, {item: 'Kabibisa kiti', value: 2 } ];
//     prettyPrintRows(rows, headers);
//
// })();
