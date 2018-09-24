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
const __1 = require("../");
const alsatian_1 = require("alsatian");
const path_1 = require("path");
let ReaderSpec = class ReaderSpec {
    setup() {
        this.subject = new __1.norc.Reader(path_1.join(__dirname, './test_files/test_data.orc'));
    }
    readerNoOptions() {
        return __awaiter(this, void 0, void 0, function* () {
            yield new Promise((resolve, reject) => {
                this.subject.on('data', chunk => {
                    chunk = JSON.parse(chunk);
                    alsatian_1.Expect(Array.isArray(chunk)).toBeTruthy();
                    alsatian_1.Expect(Object.keys(chunk[0]).length).toEqual(14);
                    return resolve();
                });
                this.subject.on('error', e => {
                    return reject(e);
                });
                this.subject.read();
            });
        });
    }
    readerIterator() {
        this.subject.read((err, it) => {
            let row = it.next();
            alsatian_1.Expect(Object.keys(row.value).length).toEqual(14);
        });
    }
    readSelected(columns) {
        this.subject.read({ columns }, (err, it) => {
            if (err) {
            }
            else {
                let row = it.next();
                alsatian_1.Expect(Object.keys(row).every(value => columns.includes(value))).toBeTruthy();
                for (let item in row.value) {
                    switch (item) {
                        case 'LoanId': {
                            alsatian_1.Expect(typeof (item.LoanId)).toEqual('string');
                            break;
                        }
                        case 'ProductType': {
                            alsatian_1.Expect(typeof (item.ProductType)).toEqual('string');
                            break;
                        }
                        case 'LoanTermMonths': {
                            alsatian_1.Expect(typeof (item.LoanTermMonths)).toEqual('number');
                            break;
                        }
                        case 'APR': {
                            alsatian_1.Expect(typeof item.APR).toEqual('number');
                            break;
                        }
                        case 'FundingDate': {
                            alsatian_1.Expect(typeof item.FundingDate).toEqual('string');
                            break;
                        }
                        case 'ActualPurchasePercentage': {
                            alsatian_1.Expect(typeof item.ActualPurchasePercentage).toEqual('number');
                            break;
                        }
                        case 'UnpaidPrincipalBalanceAtPurchaseDate': {
                            alsatian_1.Expect(typeof item.UnpaidPrincipalBalanceAtPurchaseDate).toEqual('number');
                            break;
                        }
                        case 'TotalPurchaseAmount': {
                            alsatian_1.Expect(typeof item.TotalPurchaseAmount).toEqual('number');
                            break;
                        }
                        case 'State': {
                            alsatian_1.Expect(typeof item.State).toEqual('string');
                            break;
                        }
                        case 'QualifyingFICO': {
                            alsatian_1.Expect(typeof item.QualifyingFICO).toEqual('number');
                            break;
                        }
                        case 'QualifyingDTI': {
                            alsatian_1.Expect(typeof item.QualifyingDTI).toEqual('number');
                            break;
                        }
                        case 'Installer': {
                            alsatian_1.Expect(typeof item.Installer).toEqual('string');
                            break;
                        }
                        case 'Tranche': {
                            alsatian_1.Expect(typeof item.Tranche).toEqual('string');
                            break;
                        }
                    }
                }
            }
        });
    }
};
__decorate([
    alsatian_1.SetupFixture
], ReaderSpec.prototype, "setup", null);
__decorate([
    alsatian_1.AsyncTest("Read all contents")
], ReaderSpec.prototype, "readerNoOptions", null);
__decorate([
    alsatian_1.Test("Read all into iterator")
], ReaderSpec.prototype, "readerIterator", null);
__decorate([
    alsatian_1.Test("Read selected columns"),
    alsatian_1.TestCase(["UnpaidPrincipalBalanceAtPurchaseDate"]),
    alsatian_1.TestCase(["LoanId", "Tranche"]),
    alsatian_1.TestCase(["QualifyingFICO", "QualifyingDTI", "Installer", "APR"])
], ReaderSpec.prototype, "readSelected", null);
ReaderSpec = __decorate([
    alsatian_1.TestFixture("Reader Tests")
], ReaderSpec);
exports.ReaderSpec = ReaderSpec;
//# sourceMappingURL=reader.js.map