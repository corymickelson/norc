const {DataType, norc} = require('.')
const {fromPath} = require('fast-csv')
let file = new norc.Writer('/tmp/norc.test.orc')
const schema = {
    LoanId: DataType.STRING,
    ProductType: DataType.STRING,
    LoanTermMonths: DataType.INT,
    APR: DataType.STRING,
    FundingDate: DataType.DATE,
    ActualPurchasePercentage: DataType.FLOAT,
    UnpaidPrincipalBalanceAtPurchaseDate: DataType.DATE,
    TotalPurchaseAmount: DataType.FLOAT,
    State:DataType.STRING,
    QualifyingFICO:DataType.INT,
    QualifyingDTI:DataType.FLOAT,
    Installer:DataType.STRING,
    Partner:DataType.STRING,
    Tranche:DataType.STRING
}
let loanId = [], productType = [], loanTermMonths = [], apr = [], fundingDate = [], actualPurchasePercentage = [],
    unpaidBalance = [], total = [], state = [], fico = [], dti = [], installer = [], partner = [], tranche = []
file.schema(schema)
fromPath(require('path').join(__dirname, './carval_details.csv'))
    .on('data', chunk => {
        loanId.push(chunk[0])
        productType.push(chunk[1])
        loanTermMonths.push(parseInt(chunk[2]))
        apr.push(chunk[3])
        fundingDate.push(chunk[4])
        actualPurchasePercentage.push(parseFloat(chunk[5]))
        unpaidBalance.push(chunk[6])
        total.push(parseFloat(chunk[7]))
        state.push(chunk[8])
        fico.push(parseInt(chunk[9]))
        dti.push(parseFloat(chunk[10]))
        installer.push(chunk[11])
        partner.push(chunk[12])
        tranche.push(chunk[13])
    })
    .on('end', () => {
        file.add('LoanId', loanId)
        file.add('ProductType', productType)
        file.add('LoanTermMonths', loanTermMonths)
        file.add('APR', apr)
        file.add('FundingDate', fundingDate)
        file.add('ActualPurchasePercentage', actualPurchasePercentage)
        file.add('UnpaidPrincipalBalanceAtPurchaseDate', unpaidBalance)
        file.add('TotalPurchaseAmount', total)
        file.add('State', state)
        file.add('QualifyingFICO', fico)
        file.add('QualifyingDTI', dti)
        file.add('Installer', installer)
        file.add('Partner', partner)
        file.add('Tranche', tranche)
        file.close()
    })

//"struct<LoanId:string,ProductType:string,LoanTermMonths:smallint,APR:string,FundingDate:date,ActualPurchasePercentage:float,UnpaidPrincipalBalanceAtPurchaseDate:date,TotalPurchasAamount:float,State:string,QualifyingFICO:int,QualifyingDTI:float,Installer:string,Partner:string,Tranche:string>" /tmp/carval_details.csv /tmp/carval_details.orc

