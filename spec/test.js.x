const { ok, equal, notEqual, doesNotThrow } = require('assert')
const { readFileSync } = require('fs')
const { DataType, norc } = require('.')
const { fromPath } = require('fast-csv')
const { join } = require('path')
if (!global.gc) {
    global.gc = () => { }
}

function writer() {
    return new Promise((resolve, reject) => {

        let file = new norc.Writer('/tmp/norc.test.orc')
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
        let i = 0
        fromPath(join(__dirname, './carval_details.csv'))
            .on('data', chunk => {
                file.add({
                    LoanId: chunk[0],
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
                i++
            })
            .on('end', () => {
                file.close()
                ok(readFileSync('/tmp/norc.test.orc'))
                return resolve()
            })
    })
}

function readerOptsColumns() {
    return new Promise((resolve, reject) => {
        const reader = new norc.Reader('/tmp/norc.test.orc')
        reader.read({ columns: ['LoanId', 'FundingDate'] }, (err, it) => {
            if (err) {
                console.error(err)
            } else {
                let i = it.next()
                equal(Object.keys(i.value).length, 2, 'Only selected columns returned')
                console.log('Reader with Column options complete')
                return resolve()
            }
        })
    })
}
function readerEvent() {
    return new Promise((resolve, reject) => {
        const reader = new norc.Reader('/tmp/norc.test.orc')
        reader.on('data', chunk => {
            ok(chunk)
            doesNotThrow(() => JSON.parse(chunk))
        })
        reader.on('end', () => {
            console.log('Reader Event Emitter Complete')
            return resolve()
        })
        reader.read()
        global.gc()
    })
}

async function runTests() {
    await writer()
    global.gc()
    await readerOptsColumns()
    global.gc()
    await readerEvent()
}

runTests()
    .then(() => { process.exit(0) })
    .catch(err => { console.error(err); process.exit(1) })

//"struct<LoanId:string,ProductType:string,LoanTermMonths:smallint,APR:string,FundingDate:date,ActualPurchasePercentage:float,UnpaidPrincipalBalanceAtPurchaseDate:date,TotalPurchasAamount:float,State:string,QualifyingFICO:int,QualifyingDTI:float,Installer:string,Partner:string,Tranche:string>" /tmp/carval_details.csv /tmp/carval_details.orc

