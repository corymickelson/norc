/// <reference types="node" />

import {EventEmitter} from "events";

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
    export class Reader extends EventEmitter {
        readonly type: string
        readonly writeVersion: string
        readonly formatVersion: string
        readonly compressions: string

        constructor(input: string)

        /**
         * Read all contents into memory and emit on Reader.on('data', (data:string) => void): void
         */
        read(): void
        /**
         * Read all contents into memory, if a callback is defined an iterator will be returned otherwise listen .on('data') for content chunks
         */
        read(cb:(err:Error, data: Iterator<object>|null) => void): void
        /**
         * Read only columns defined in opts.columns, get contents back as either an event(data:string) or an array iterator
         * The iterator will return the contents as an object, whereas the event will return a string which will subsequently need to be JSON parse(d).
         * The event is more performant, but the iterator offers more control.
         * @param opts
         * @param cb
         */
        read(opts: {resultType?: 'iterator'|'event', columns?: string[]}, cb?:(err:Error, data: Iterator<object>|null) => void): void

        columnStatistics(column:string): string
    }
    export class Writer {
        constructor(output: string)
        stringTypeSchema(schema: string): void
        fromCsv(file: string, cb: (err: Error, norc: Writer) => void): void
        schema(v: {[key:string]: DataType}): void
        add(row: {[key:string]: string|boolean|number}): void
        add(rows: {[key:string]: string|boolean|number}[]): void
        close(): void
    }
}