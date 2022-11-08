#! /usr/bin/env node

const { ArgumentParser } = require('argparse');
const Papa = require('papaparse');
const events = require('events');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { readGedcom } = require('read-gedcom');

const parser = new ArgumentParser({
  description: 'Converts a gedcom file into relational CSVs',
});

parser.add_argument('-i', '--input', {
  help: 'input gedcom file',
  required: true,
});
parser.add_argument('-c', '--csv', {
  help: 'output relational CSV directory',
});
parser.add_argument('-r', '--rdf', {
  help: 'output RDF file',
});
parser.add_argument('-d', '--force-date-parsing', {
  nargs: '+',
  help: `When Javascript date parsing fails, a list \
of delimiters to split by (e.g. "Abt. 1780 - 1790" \
would be successfully parsed with "-d abt. -"). \
If this option is used, date values are omitted \
when parsing is unsuccessful`,
});
parser.add_argument('-p', '--parents-table', {
  action: 'store_true',
  help: `Generate an additional PARENTS table, \
directly connecting children to parents`,
});
parser.add_argument('-n', '--copy_individual_names', {
  action: 'store_true',
  help: `Store a copy of the main NAME for each \
INDI entry`,
});

const args = parser.parse_args();
const FORCE_DATE_REGEX = args['force_date_parsing']
  ? new RegExp(
      args['force_date_parsing']
        .map((delimiter) => {
          return `(?:\s*${delimiter}\s*)`;
        })
        .join('|'),
      'gi'
    )
  : null;

if (!fs.existsSync(args.input)) {
  throw new Error('Input file does not exist');
}

const EXTRA_TABLES = {
  ...(args['parents_table']
    ? { PARENTS: { rows: [], columns: new Set(['child', 'parent']) } }
    : {}),
};
const EXTRA_TABLE_PARSERS = {
  ...(args['parents_table']
    ? {
        PARENTS: (record, tag, pointer, children) => {
          const results = [];
          if (tag === 'FAM') {
            children.forEach((childRecord) => {
              if (childRecord.tag === 'CHIL') {
                if (record.WIFE) {
                  results.push({
                    child: childRecord.value,
                    parent: record.WIFE,
                  });
                }
                if (record.HUSB) {
                  results.push({
                    child: childRecord.value,
                    parent: record.HUSB,
                  });
                }
              }
            });
          }
          return results;
        },
      }
    : {}),
};

const LINK_ATTRIBUTES = {
  CHIL: 'INDI',
  FAMC: 'FAM',
  FAMS: 'FAM',
};

const PROMOTED_VALUES = {
  INDI: new Set([
    'TITL',
    'RELI',
    '_EMPLOY',
    'BLES',
    '_DCAUSE',
    'DEAT',
    'OCCU',
    'EDUC',
    '_MDCL',
  ]),
};

const formatValue = (tag, value) => {
  switch (tag) {
    case 'DATE':
      try {
        return new Date(value).toISOString().split('T')[0];
      } catch (err) {
        if (args['force_date_parsing']) {
          const numbers = value
            .split(FORCE_DATE_REGEX)
            .map((maybeDate) => Number(new Date(maybeDate)))
            .filter((millis) => !isNaN(millis));
          if (numbers.length > 0) {
            const mean =
              numbers.reduce((agg, value) => agg + value, 0) / numbers.length;
            return new Date(mean).toISOString().split('T')[0];
          } else {
            return undefined;
          }
        }
        return value;
      }
    default:
      return value;
  }
};

fs.readFile(args.input, (error, buffer) => {
  if (error) throw error;
  const gedcom = readGedcom(buffer);
  const allRecords = Array.from(gedcom.get().array());

  console.log('Identifying available data tables...');
  const baseTableNames = allRecords.reduce((agg, gedcomRecord) => {
    agg.add(gedcomRecord.tag);
    return agg;
  }, new Set());

  const tables = {
    ...EXTRA_TABLES,
  };
  const promotedValueTables = {};

  const addJunctionRow = (sourceTable, sourceId, targetTable, targetId) => {
    const junctionTable = `${sourceTable}_${targetTable}`;
    if (!tables[junctionTable]) {
      tables[junctionTable] = {
        rows: [],
        columns: new Set([sourceTable, targetTable]),
      };
    }
    tables[junctionTable].rows.push({
      [sourceTable]: sourceId,
      [targetTable]: targetId,
    });
  };

  const addPromotedValue = (
    sourceTable,
    sourceId,
    targetTable,
    targetValue
  ) => {
    if (!promotedValueTables[targetTable]) {
      promotedValueTables[targetTable] = {
        rows: {},
        columns: new Set(['id', targetTable]),
      };
    }
    if (!promotedValueTables[targetTable].rows[targetValue]) {
      const id = `@gedcom-to-csv_generated_${targetTable}${
        Object.keys(promotedValueTables[targetTable].rows).length
      }@`;
      promotedValueTables[targetTable].rows[targetValue] = {
        id,
        [targetTable]: targetValue,
      };
    }
    addJunctionRow(
      sourceTable,
      sourceId,
      targetTable,
      promotedValueTables[targetTable].rows[targetValue].id
    );
  };

  const parsePromotedRecord = (sourceTable, sourceId, promotedRecord) => {
    if (!tables[promotedRecord.tag]) {
      tables[promotedRecord.tag] = {
        rows: [],
        columns: new Set(['id', promotedRecord.tag]),
      };
    }
    const id =
      promotedRecord.pointer ||
      `@${promotedRecord.tag}${tables[promotedRecord.tag].rows.length}@`;
    addJunctionRow(sourceTable, sourceId, promotedRecord.tag, id);

    const row = {
      id,
      [promotedRecord.tag]: formatValue(
        promotedRecord.tag,
        promotedRecord.value
      ),
    };
    promotedRecord.children?.forEach((childRecord) => {
      parseChild(promotedRecord.tag, id, row, childRecord);
    });

    tables[promotedRecord.tag].rows.push(row);
  };

  const parseChild = (tag, pointer, record, childRecord) => {
    if (LINK_ATTRIBUTES[childRecord.tag]) {
      addJunctionRow(
        tag,
        pointer,
        LINK_ATTRIBUTES[childRecord.tag],
        childRecord.value
      );
    } else if (baseTableNames.has(childRecord.tag)) {
      addJunctionRow(tag, pointer, childRecord.tag, childRecord.value);
    } else if (PROMOTED_VALUES[tag]?.has(childRecord.tag)) {
      addPromotedValue(tag, pointer, childRecord.tag, childRecord.value);
    } else if (childRecord.children?.length > 0) {
      parsePromotedRecord(tag, pointer, childRecord);
    } else if (childRecord.value === null) {
      console.log(
        `Skipping null child value when parsing ${tag}:`,
        childRecord
      );
    } else {
      let dupIndex = 2;
      let column = childRecord.tag;
      while (record[column] !== undefined) {
        column = `${childRecord.tag}${dupIndex}`;
        dupIndex += 1;
      }
      record[column] = formatValue(childRecord.tag, childRecord.value);
      tables[tag].columns.add(column);
    }
  };

  console.log('Parsing records...');
  allRecords.forEach((gedcomRecord) => {
    const { tag, pointer, children } = gedcomRecord;
    if (!tables[tag]) {
      tables[tag] = { rows: [], columns: new Set(['id']) };
    }
    const record = { id: pointer };
    children.forEach((childRecord) => {
      parseChild(tag, pointer, record, childRecord);
      if (
        args['copy_individual_names'] &&
        tag === 'INDI' &&
        childRecord.tag === 'NAME'
      ) {
        tables.INDI.columns.add('NAME');
        record.NAME = childRecord.value;
      }
    });
    tables[tag].rows.push(record);

    Object.entries(EXTRA_TABLE_PARSERS).forEach(([table, parser]) => {
      tables[table].rows.push(...parser(record, tag, pointer, children));
    });
  });

  console.log('Integrating promoted value tables...');
  Object.entries(promotedValueTables).forEach(([table, { rows, columns }]) => {
    if (tables[table]) {
      tables[table].columns.add(...columns);
      tables[table].rows.push(...Object.values(rows));
    } else {
      tables[table] = { rows: Object.values(rows), columns };
    }
  });

  if (args.csv) {
    console.log('Writing tables to disk...');
    Object.entries(tables).forEach(([table, { rows, columns }]) => {
      const csvString = Papa.unparse(rows, {
        columns: Array.from(columns),
      });
      fs.writeFile(
        path.join(args.csv, `${table}.csv`),
        csvString,
        {},
        () => {}
      );
    });
  }

  if (args.rdf) {
    console.error('RDF output not yet supported');
  }
});
