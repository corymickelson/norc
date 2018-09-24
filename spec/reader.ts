import { norc } from '../'
import { TestFixture, SetupFixture, AsyncTest, Expect, Test, TestCase } from 'alsatian'
import Reader = norc.Reader;
import { join } from "path";

@TestFixture("Reader Tests")
export class ReaderSpec {
    private subject?: Reader

    @SetupFixture
    public setup() {
        this.subject = new norc.Reader(join(__dirname, './test_files/test_data.orc'))
    }

    @AsyncTest("Read all contents")
    public async readerNoOptions() {
        await new Promise((resolve, reject) => {
            (this.subject as Reader).on('data', chunk => {
                chunk = JSON.parse(chunk)
                Expect(Array.isArray(chunk)).toBeTruthy()
                Expect(Object.keys(chunk[0]).length).toEqual(14)
                return resolve()
            });
            (this.subject as Reader).on('error', e => {
                return reject(e)
            });
            (this.subject as Reader).read()
        })
    }

    @Test("Read all into iterator")
    public readerIterator() {
        (this.subject as Reader).read((err, it) => {
            let row = (it as Iterator<object>).next()
            Expect(Object.keys(row.value).length).toEqual(14)
        })
    }

    @Test("Read selected columns")
    @TestCase(["UnpaidPrincipalBalanceAtPurchaseDate"])
    @TestCase(["LoanId", "Tranche"])
    @TestCase(["QualifyingFICO", "QualifyingDTI", "Installer", "APR"])
    public readSelected(columns: string[]) {
        (this.subject as Reader).read({ columns }, (err, it) => {
            if (err) {
            }
            else {
                let row = (it as Iterator<object>).next()
                Expect(Object.keys(row).every(value => columns.includes(value))).toBeTruthy()
                for (let item in row.value) {
                    switch (item) {
                        case 'LoanId': {
                            Expect(typeof ((item as any).LoanId)).toEqual('string')
                            break
                        }
                        case 'ProductType': {
                            Expect(typeof ((item as any).ProductType)).toEqual('string')
                            break
                        }
                        case 'LoanTermMonths': {
                            Expect(typeof ((item as any).LoanTermMonths)).toEqual('number')
                            break
                        }
                        case 'APR': {
                            Expect(typeof (item as any).APR).toEqual('number')
                            break
                        }
                        case 'FundingDate': {
                            Expect(typeof (item as any).FundingDate).toEqual('string')
                            break
                        }
                        case 'ActualPurchasePercentage': {
                            Expect(typeof (item as any).ActualPurchasePercentage).toEqual('number')
                            break
                        }
                        case 'UnpaidPrincipalBalanceAtPurchaseDate': {
                            Expect(typeof (item as any).UnpaidPrincipalBalanceAtPurchaseDate).toEqual('number')
                            break
                        }
                        case 'TotalPurchaseAmount': {
                            Expect(typeof (item as any).TotalPurchaseAmount).toEqual('number')
                            break
                        }
                        case 'State': {
                            Expect(typeof (item as any).State).toEqual('string')
                            break
                        }
                        case 'QualifyingFICO': {
                            Expect(typeof (item as any).QualifyingFICO).toEqual('number')
                            break
                        }
                        case 'QualifyingDTI': {
                            Expect(typeof (item as any).QualifyingDTI).toEqual('number')
                            break
                        }
                        case 'Installer': {
                            Expect(typeof (item as any).Installer).toEqual('string')
                            break
                        }
                        case 'Tranche': {
                            Expect(typeof (item as any).Tranche).toEqual('string')
                            break
                        }
                    }
                }
            }
        })
    }

}

