export declare class ReaderSpec {
    private subject?;
    setup(): void;
    readerNoOptions(): Promise<void>;
    readerIterator(): void;
    readSelected(columns: string[]): void;
}
