import { ParquetModel, ParquetColumn } from "@frequency/parquet" // right?
import { write } from "fs";
import { Schema } from "inspector";
import Int64 from "node-int64";
import { ParquetEnvelopeWriter } from "../parquet";
import { ParquetSchema, FieldDefinition } from "./schema";
import {ParquetWriter} from "./writer";

export class ParquetWriterBuilder {
    private readonly _parquetWriter: ParquetWriter;

    constructor(modeldata: ParquetModel, opts: any) {
        opts = {} || opts;
        let emptyFn = () => {};

        let schema = this.createParquetSchema(modeldata, opts);
        let envelope = new ParquetEnvelopeWriter(schema, emptyFn, emptyFn, new Int64(0), opts )
        this._parquetWriter = new ParquetWriter (schema, opts)
    }

    createParquetSchema (modeldata: ParquetModel, opts: {}): ParquetSchema {
        let fields = new FieldDefinition({});
        modeldata.forEach((column: ParquetColumn) => {
            fields.add({
                type: column.column_type, //ParquetType
                compression: column.compression, //ParquetCompression: ColumnCompressionCodec
            })
        });
        return new ParquetSchema(fields);
    }

    buildParquetEnvelopeWriter () {

    }

    build(): ParquetWriter {
        return this._parquetWriter;
    }

    // pub struct ParquetColumn {
    //     /// The label for what this column represents
    //     name: String,
    //     /// Parquet type labels
    //     column_type: ParquetType,
    //     /// Compression for column
    //     compression: ColumnCompressionCodec,
    //     /// Whether or not to use a bloom filter
    //     bloom_filter: bool,
    // }
}