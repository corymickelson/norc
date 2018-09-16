const addon = require('bindings')('norc')
exports.norc = addon
Object.defineProperty(exports, "__esModule", {value: true})
let DataType;
(function(DataType) {

    DataType[DataType["BOOLEAN"] = 0] = "BOOLEAN";
    DataType[DataType["TINYINT"] = 1] = "TINYINT";
    DataType[DataType["SMALLINT"] = 2] = "SMALLINT";
    DataType[DataType["INT"] = 3] = "INT"
    DataType[DataType["BIGINT"] = 4] = "BIGINT";
    DataType[DataType["FLOAT"] = 5] = "FLOAT"
    DataType[DataType["DOUBLE"] = 6] = "DOUBLE"
    DataType[DataType["STRING"] = 7] = "STRING"
    DataType[DataType["BINARY"] = 8] = "BINARY"
    DataType[DataType["TIMESTAMP"] = 9] = "TIMESTAMP"
    DataType[DataType["ARRAY"] = 10] = "ARRAY"
    DataType[DataType["MAP"] = 11] = "MAP"
    DataType[DataType["STRUCT"] = 12] = "STRUCT"
    DataType[DataType["UNION"] = 13] = "UNION"
    DataType[DataType["DECIMAL"] = 14] = "DECIMAL"
    DataType[DataType["DATE"] = 15] = "DATE"
    DataType[DataType["VARCHAR"] = 16] = "VARCHAR"
    DataType[DataType["CHAR"] = 17] = "CHAR"
})(DataType = exports.DataType || (exports.DataType = {}));