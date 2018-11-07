import {DataType, norc} from '../'
import {TestFixture, SetupFixture, AsyncTest, Expect, Test, TestCase, Timeout} from 'alsatian'
import Reader = norc.Reader;
import {join} from "path";
import Writer = norc.Writer;


@TestFixture("Reader Tests")
export class ReaderSpec {
    private subject?: Reader
    private eventLength: number = 0
    private iteratorLength: number = 0

    @SetupFixture
    @Timeout(50000)
    public setup() {
        this.subject = new norc.Reader(join(__dirname, './test_files/test_data.orc'))
    }

    @AsyncTest("Read all contents")
    public async readerNoOptions() {
        await new Promise((resolve, reject) => {
            (this.subject as Reader).on('data', chunk => {
                chunk = JSON.parse(chunk)
                this.eventLength += chunk.length
                Expect(Array.isArray(chunk)).toBeTruthy()
                Expect(Object.keys(chunk[0]).length).toEqual(14)
            });
            (this.subject as Reader).on('error', e => {
                return reject(e)
            });
            (this.subject as Reader).on('end', e => {
                return resolve()
            });
            (this.subject as Reader).read()
        })
    }

    @AsyncTest("Read all into iterator")
    public async readerIterator() {
        await new Promise((resolve, reject) => {
            // @ts-ignore
            (this.subject as Reader).read((err: Error, it: Iterator<object>) => {
                let i = it.next()
                while (!i.done) {
                    Expect(Object.keys(i.value).length).toEqual(14)
                    this.iteratorLength += 1
                    i = it.next()
                }
                if(i.done) {
                    Expect(this.iteratorLength).toEqual(this.eventLength)
                }
                return resolve()
            })
        })
    }

    @Test("Read selected columns")
    @TestCase(["UnpaidPrincipalBalanceAtPurchaseDate"])
    @TestCase(["LoanId", "Tranche"])
    @TestCase(["QualifyingFICO", "QualifyingDTI", "Installer", "APR"])
    public readSelected(columns: string[]) {
        (this.subject as Reader).read({columns}, (err, it) => {
            if (err) {
            }
            else {
                let row = (it as Iterator<object>).next()
                Expect(Object.keys(row.value).every(value => columns.includes(value))).toBeTruthy()
                for (let item in row.value) {
                    // @ts-ignore
                    if(!row.value[item]) continue
                    switch (item) {
                        case 'LoanId': {
                            Expect(typeof (row.value as any)[item]).toEqual('string')
                            break
                        }
                        case 'ProductType': {
                            Expect(typeof (row.value as any)[item]).toEqual('string')
                            break
                        }
                        case 'LoanTermMonths': {
                            Expect(typeof (row.value as any)[item]).toEqual('number')
                            break
                        }
                        case 'APR': {
                            Expect(typeof (row.value as any)[item]).toEqual('number')
                            break
                        }
                        case 'FundingDate': {
                            Expect(typeof (row.value as any)[item]).toEqual('string')
                            break
                        }
                        case 'ActualPurchasePercentage': {
                            Expect(typeof (row.value as any)[item]).toEqual('number')
                            break
                        }
                        case 'UnpaidPrincipalBalanceAtPurchaseDate': {
                            Expect(typeof (row.value as any)[item]).toEqual('number')
                            break
                        }
                        case 'TotalPurchaseAmount': {
                            Expect(typeof (row.value as any)[item]).toEqual('number')
                            break
                        }
                        case 'State': {
                            Expect(typeof (row.value as any)[item]).toEqual('string')
                            break
                        }
                        case 'QualifyingFICO': {
                            Expect(typeof (row.value as any)[item]).toEqual('number')
                            break
                        }
                        case 'QualifyingDTI': {
                            Expect(typeof (row.value as any)[item]).toEqual('number')
                            break
                        }
                        case 'Installer': {
                            Expect(typeof (row.value as any)[item]).toEqual('string')
                            break
                        }
                        case 'Tranche': {
                            Expect(typeof (row.value as any)[item]).toEqual('string')
                            break
                        }
                    }
                }
            }
        })
    }

    @AsyncTest('Event Reader Single Entry')
    @Timeout(500000)
    public async singleReader() {
        return new Promise(resolve => {
            const file = new Writer()
            const schema = {
                key: DataType.INT,
                value: DataType.STRING
            }
            file.schema(schema)
            // @ts-ignore
            file.add({key: 0, value: 'zero'})
            file.close()
            const src = file.data()
            const reader = new Reader(src)
            reader.on('data', chunk => {
                chunk = JSON.parse(chunk)
                Expect(chunk.length).toEqual(1)
            })
            reader.on('error', e => {
                Expect.fail(e)
            })
            reader.on('end', () => {
                return resolve()
            })
            reader.read()
        })
    }

}

