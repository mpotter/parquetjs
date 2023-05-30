import fs from 'fs';
import path from 'path';
import { assert, expect } from "chai";

import { ParquetSchema, ParquetWriter, ParquetReader } from '../parquet';
import { FrequencySchema } from '../lib/frequencySchema';

const update = false;
// Super Simple snapshot testing
const checkSnapshot = (actual: any, snapshot: string, update = false) => {
  if (update) {
    fs.writeFileSync(path.resolve("test", snapshot), JSON.stringify(JSON.parse(JSON.stringify(actual)), null, 2) + "\n");
    expect(`Updated the contents of "${snapshot}"`).to.equal("");
  } else {
    const expected = require(snapshot);
    expect(JSON.parse(JSON.stringify(actual))).to.deep.equal(expected);
  }
}

const broadcastFrequencySchema: FrequencySchema = [
  {
    "name": "announcementType",
    "column_type": {
      "INTEGER": {
        "bit_width": 32,
        "sign": true
      }
    },
    "compression": "GZIP",
    "bloom_filter": false
  },
  {
    "name": "contentHash",
    "column_type": "BYTE_ARRAY",
    "compression": "GZIP",
    "bloom_filter": true
  },
  {
    "name": "fromId",
    "column_type": {
      "INTEGER": {
        "bit_width": 64,
        "sign": false
      }
    },
    "compression": "GZIP",
    "bloom_filter": true
  },
  {
    "name": "url",
    "column_type": "STRING",
    "compression": "GZIP",
    "bloom_filter": false
  }
];

describe("Frequency Schema Conversion", function () {
  it("Basic Schema", function () {
    const [schema, options] = ParquetSchema.fromFrequencySchema(broadcastFrequencySchema);
    checkSnapshot(schema, './test-files/frequency-broadcast.schema.snapshot.json', update);
    expect(options).to.deep.equal({
      "bloomFilters": [
        { "column": "contentHash" },
        { "column": "fromId" },
      ]
    });
  });

  it("Time Schema", function () {
    const [schema] = ParquetSchema.fromFrequencySchema([
      {
        name: "micro",
        column_type: {
          TIMESTAMP: {
            unit: "MICROS",
            is_adjusted_to_utc: true,
          },
        },
        compression: "UNCOMPRESSED",
        bloom_filter: false,
      },
      {
        name: "milli",
        column_type: {
          TIMESTAMP: {
            unit: "MILLIS",
            is_adjusted_to_utc: true,
          },
        },
        compression: "UNCOMPRESSED",
        bloom_filter: false,
      },
      {
        name: "time micro",
        column_type: {
          TIME: {
            unit: "MICROS",
            is_adjusted_to_utc: true,
          },
        },
        compression: "UNCOMPRESSED",
        bloom_filter: false,
      },
      {
        name: "time milli",
        column_type: {
          TIME: {
            unit: "MILLIS",
            is_adjusted_to_utc: true,
          },
        },
        compression: "UNCOMPRESSED",
        bloom_filter: false,
      },
      {
        name: "date",
        column_type: "DATE",
        compression: "UNCOMPRESSED",
        bloom_filter: false,
      }
    ]);
    checkSnapshot(schema, './test-files/frequency-timestamp.schema.snapshot.json', update);
  });
});

describe("Frequency Schema Conversion Test File", async function () {
  const [parquetSchema, writerOptions] = ParquetSchema.fromFrequencySchema(broadcastFrequencySchema);

  const row1 = {
    announcementType: 2,
    contentHash: "0x12345678",
    fromId: 12n,
    url: "https://github.com/LibertyDSNP/parquetjs/",
  };

  let reader: ParquetReader;

  before(async function () {
    const filename = 'frequency-schema-test-file.parquet';
    const writer = await ParquetWriter.openFile(parquetSchema, filename, writerOptions);
    await writer.appendRow(row1);
    await writer.close();

    reader = await ParquetReader.openFile(filename);
  });

  it('schema is generated correctly', async function () {
    checkSnapshot(parquetSchema, './test-files/frequency-schema-test-file.schema.snapshot.json', update);
  });

  it('schema is encoded correctly', async function () {
    const schema = reader.metadata?.schema;
    checkSnapshot(reader.metadata?.schema, './test-files/frequency-schema-test-file.snapshot.json', update);
  });

  it('output matches input', async function () {
    const cursor = reader.getCursor();
    const row = await cursor.next();
    const rowData = {
      ...row1,
      contentHash: Buffer.from([48, 120, 49, 50, 51, 52, 53, 54, 55, 56])
    };
    assert.deepEqual(row, rowData);
  });
});
