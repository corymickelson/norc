/// <reference types="node" />

export enum DataType{
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    STRING,
    BINARY,
    TIMESTAMP,
    ARRAY,
    MAP,
    STRUCT,
    UNION,
    DECIMAL,
    DATE,
    VARCHAR,
    CHAR
}

export namespace norc {
    export class Writer {
        constructor(output: string)
        stringTypeSchema(schema: string): void
        fromCsv(file: string, cb: (err: Error, norc: Writer) => void): void
        schema(v: {[key:string]: DataType}): void
        add(row: {[key:string]: string|boolean|number}): void
        close(): void
    }
}