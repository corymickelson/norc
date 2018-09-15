/// <reference types="node" />

export enum DataType {
    BOOLEAN = 0,
    BYTE = 1,
    SHORT = 2,
    INT = 3,
    LONG = 4,
    FLOAT = 5,
    DOUBLE = 6,
    STRING = 7,
    BINARY = 8,
    TIMESTAMP = 9,
    LIST = 10,
    MAP = 11,
    STRUCT = 12,
    UNION = 13,
    DECIMAL = 14,
    DATE = 15,
    VARCHAR = 16,
    CHAR = 17
}

export namespace norc {
    export class Writer {
        constructor(output: string)
        stringTypeSchema(schema: string): void
        // importCSV(file: string, cb: (err: Error, norc: Writer) => void): void
        schema(v: {[key:string]: DataType}): void
        add(column: string, values: Array<string|boolean|number>): void
        close(): void
    }
}