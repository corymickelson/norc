import {TestFixture, AsyncTest, Expect, Timeout} from 'alsatian'
import {DataType, norc} from '../'
import {fromPath} from 'fast-csv'
import {join} from 'path'
import Writer = norc.Writer;

@TestFixture("Writer Tests")
export class WriterSpec {

    @AsyncTest("Convert from csv")
    public async fromCsv() {
        return new Promise((resolve, reject) => {
            let filePath = join(__dirname, './test_files/test_data.orc')
            let file = new norc.Writer(filePath)
            const schema = {
                LoanId: DataType.STRING,
                ProductType: DataType.STRING,
                LoanTermMonths: DataType.INT,
                APR: DataType.FLOAT,
                FundingDate: DataType.DATE,
                ActualPurchasePercentage: DataType.FLOAT,
                UnpaidPrincipalBalanceAtPurchaseDate: DataType.FLOAT,
                TotalPurchaseAmount: DataType.FLOAT,
                State: DataType.STRING,
                QualifyingFICO: DataType.INT,
                QualifyingDTI: DataType.FLOAT,
                Installer: DataType.STRING,
                Partner: DataType.STRING,
                Tranche: DataType.STRING
            }

            file.schema(schema)
            fromPath(join(__dirname, './test_files/test_data.csv'))
                .on('data', chunk => {
                    file.add({
                        LoanId: chunk[0].replace('18', '28').replace('00', '11'),
                        ProductType: chunk[1].trim(),
                        LoanTermMonths: chunk[2].replace(/[A-Z]|[a-z]/g, '') === '' ? null : parseInt(chunk[2]),
                        APR: chunk[3].replace(/[A-Z]|[a-z]/g, '') === '' ? null : parseFloat(chunk[3]),
                        FundingDate: chunk[4],
                        ActualPurchasePercentage: chunk[5].replace(/[A-Z]|[a-z]/g, '') === '' ? null : parseFloat(chunk[5]),
                        UnpaidPrincipalBalanceAtPurchaseDate: chunk[6].replace(/[A-Z]|[a-z]/g, '') === '' ? null : parseFloat(chunk[6]),
                        TotalPurchaseAmount: chunk[7].replace(/[A-Z]|[a-z]/g, '') === '' ? null : parseFloat(chunk[7]),
                        State: chunk[8].trim(),
                        QualifyingFICO: chunk[9].replace(/[A-Z]|[a-z]/g, '') === '' ? null : parseInt(chunk[9]),
                        QualifyingDTI: chunk[10].replace(/[A-Z]|[a-z]/g, '') === '' ? null : parseFloat(chunk[10]),
                        Installer: chunk[11].trim(),
                        Partner: chunk[12].trim(),
                        Tranche: chunk[13].trim()
                    })
                })
                .on('end', () => {
                    file.close()
                    return resolve()
                })
        })
    }

    @AsyncTest('Memory Writer')
    public async memoryFileWriterTest() {
        const file = new Writer()
        const schema = {
            key: DataType.INT,
            value: DataType.STRING
        }
        file.schema(schema)
        // @ts-ignore
        file.add({key: 0, value: 'zero'})
        file.close()
        Expect(file.data()).toBeDefined()
        Expect(file.data().length).toEqual(340)
    }

    @AsyncTest('Writer Merge Existing File')
    public async mergeTest() {
        const writer = new Writer()
        let filePath = join(__dirname, './test_files/test_data.orc')
        const schema = {
            LoanId: DataType.STRING,
            ProductType: DataType.STRING,
            LoanTermMonths: DataType.INT,
            APR: DataType.FLOAT,
            FundingDate: DataType.DATE,
            ActualPurchasePercentage: DataType.FLOAT,
            UnpaidPrincipalBalanceAtPurchaseDate: DataType.FLOAT,
            TotalPurchaseAmount: DataType.FLOAT,
            State: DataType.STRING,
            QualifyingFICO: DataType.INT,
            QualifyingDTI: DataType.FLOAT,
            Installer: DataType.STRING,
            Partner: DataType.STRING,
            Tranche: DataType.STRING
        }
        writer.schema(schema)
        writer.merge(filePath, i => i.State === 'TX')
        writer.close()
        const mergedFileData = writer.data()
        require('fs').writeFileSync('/tmp/test.orc', mergedFileData)
    }
}