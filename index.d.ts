/// <reference types="node" />

export namespace norc {
    export class Writer {
        constructor(output: string)
        stringTypeSchema(schema: string): void
        importCSV(file:string, cb:(err:Error, norc:Writer) => void): void
        close(): void
    }
}