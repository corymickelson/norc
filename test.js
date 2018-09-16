const {DataType, norc} = require('.')
const {fromPath} = require('fast-csv')
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
    State:DataType.STRING,
    QualifyingFICO:DataType.INT,
    QualifyingDTI:DataType.FLOAT,
    Installer:DataType.STRING,
    Partner:DataType.STRING,
    Tranche:DataType.STRING
}

file.schema(schema)
fromPath(require('path').join(__dirname, './carval_details.csv'))
    .on('data', chunk => {
        file.add({
    LoanId: chunk[0],
    ProductType: chunk[1].trim(),
    LoanTermMonths: parseInt(chunk[2]),
    APR: parseFloat(chunk[3]),
    FundingDate: chunk[4],
    ActualPurchasePercentage: parseFloat(chunk[5]),
    UnpaidPrincipalBalanceAtPurchaseDate: chunk[6] === '' ? null : parseFloat(chunk[6]),
    TotalPurchaseAmount: parseFloat(chunk[7]),
    State:chunk[8].trim(),
    QualifyingFICO:parseInt(chunk[9]),
    QualifyingDTI:chunk[10] === '' ? null : parseFloat(chunk[10]),
    Installer:chunk[11].trim(),
    Partner:chunk[12].trim(),
    Tranche:chunk[13].trim()
})
    })
    .on('end', () => {
        file.close()
    })

//"struct<LoanId:string,ProductType:string,LoanTermMonths:smallint,APR:string,FundingDate:date,ActualPurchasePercentage:float,UnpaidPrincipalBalanceAtPurchaseDate:date,TotalPurchasAamount:float,State:string,QualifyingFICO:int,QualifyingDTI:float,Installer:string,Partner:string,Tranche:string>" /tmp/carval_details.csv /tmp/carval_details.orc

