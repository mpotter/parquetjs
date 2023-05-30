// Support Frequency Parquet Schema conversion to a Parquet file
import { ParquetCompression, FieldDefinition, ParquetType, SchemaDefinition, supportedTypes } from './declare';

export type FrequencySchema = Array<ParquetColumn>;

type FrequencyParquetType = ParquetBaseType | ParquetStringType | ParquetNumericType | ParquetTemporalType;

interface ParquetColumn {
    name: string;
    column_type: FrequencyParquetType;
    compression: ColumnCompressionCodec;
    bloom_filter: boolean;
}

type ColumnCompressionCodec = ParquetCompression;

type ParquetBaseType = "BOOLEAN" | "INT32" | "INT64" | "FLOAT" | "DOUBLE" | "BYTE_ARRAY" | "FIXED_LEN_BYTE_ARRAY";

type ParquetStringType = "STRING" | "UUID";

type ParquetNumericType = ParquetInteger | ParquetDecimal;

type ParquetInteger = {
    INTEGER: {
        bit_width: number;
        sign: boolean;
    };
};

type ParquetDecimal = {
    DECIMAL: {
        scale: number;
        precision: number;
    };
};

type ParquetTemporalType = "DATE" | "INTERVAL" | ParquetTime | ParquetTimestamp;

type ParquetTime = {
    TIME: {
        is_adjusted_to_utc: boolean;
        unit: ParquetTimeUnit;
    };
};

type ParquetTimestamp = {
    TIMESTAMP: {
        is_adjusted_to_utc: boolean;
        unit: ParquetTimeUnit;
    };
};

type ParquetTimeUnit = "MILLIS" | "MICROS" | "NANOS";

/**
 * Simple check to make sure that type is supported.
 */
const isColumnTypeSupported = (incoming: string): incoming is ParquetType => {
    return supportedTypes.has(incoming);
}

/**
 * Error to capture all the unsupported edge cases
 */
export class UnsupportedFrequencySchemaError extends Error {
    constructor(msg: string) {
        const message = `Unsupported Frequency schema: ${msg}`;
        super(message);
        this.name = 'UnsupportedFrequencySchemaError';
    }
}

const convertColumnType = (columnType: FrequencyParquetType): FieldDefinition["type"] => {
    if (typeof columnType === "string") {
        if (columnType === "STRING") return "UTF8";
        if (isColumnTypeSupported(columnType)) return columnType;
        throw new UnsupportedFrequencySchemaError(columnType.toString());
    }
    // ParquetJs uses the old format still, so not all options are available
    if ("INTEGER" in columnType) {
        return `${columnType.INTEGER.sign ? "" : "U"}INT_${columnType.INTEGER.bit_width}` as ParquetType;
    }
    if ("TIMESTAMP" in columnType && columnType.TIMESTAMP.is_adjusted_to_utc && columnType.TIMESTAMP.unit !== "NANOS") {
        return `TIMESTAMP_${columnType.TIMESTAMP.unit}` as ParquetType;
    }
    if ("TIME" in columnType && columnType.TIME.is_adjusted_to_utc && columnType.TIME.unit !== "NANOS") {
        return `TIME_${columnType.TIME.unit}` as ParquetType;
    }

    throw new UnsupportedFrequencySchemaError(columnType.toString());
};

/**
 * Converts a field from a JSON Schema into a Parquet Field Definition
 */
const fromColumn = (column: ParquetColumn): FieldDefinition => {
    return {
        type: convertColumnType(column.column_type),
        compression: column.compression,
        statistics: false,
    };
}

/**
 * Converts supported Json Schemas into Parquet Schema Definitions
 */
export const fromFrequencySchema = (frequencySchema: FrequencySchema): SchemaDefinition => {
    const schema: SchemaDefinition = {};

    for (const column of frequencySchema) {
        schema[column.name] = fromColumn(column);
    }

    return schema;
}
