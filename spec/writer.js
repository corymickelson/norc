"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const alsatian_1 = require("alsatian");
const __1 = require("../");
const fast_csv_1 = require("fast-csv");
const path_1 = require("path");
let WriterSpec = class WriterSpec {
    fromCsv() {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                let filePath = path_1.join(__dirname, './test_files/test_data.orc');
                let file = new __1.norc.Writer(filePath);
                const schema = {
                    LoanId: __1.DataType.STRING,
                    ProductType: __1.DataType.STRING,
                    LoanTermMonths: __1.DataType.INT,
                    APR: __1.DataType.FLOAT,
                    FundingDate: __1.DataType.DATE,
                    ActualPurchasePercentage: __1.DataType.FLOAT,
                    UnpaidPrincipalBalanceAtPurchaseDate: __1.DataType.FLOAT,
                    TotalPurchaseAmount: __1.DataType.FLOAT,
                    State: __1.DataType.STRING,
                    QualifyingFICO: __1.DataType.INT,
                    QualifyingDTI: __1.DataType.FLOAT,
                    Installer: __1.DataType.STRING,
                    Partner: __1.DataType.STRING,
                    Tranche: __1.DataType.STRING
                };
                file.schema(schema);
                fast_csv_1.fromPath(path_1.join(__dirname, './test_files/test_data.csv'))
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
                    });
                })
                    .on('end', () => {
                    file.close();
                    // let src = file.getBuffer()
                    // console.log('src: ', src)
                    // Expect(src).toBeDefined()
                    return resolve();
                });
            });
        });
    }
};
__decorate([
    alsatian_1.AsyncTest("Convert from csv"),
    alsatian_1.Timeout(30000)
], WriterSpec.prototype, "fromCsv", null);
WriterSpec = __decorate([
    alsatian_1.TestFixture("Writer Tests")
], WriterSpec);
exports.WriterSpec = WriterSpec;
//# sourceMappingURL=writer.js.map