
//
// Created by developer on 9/12/18.
//

#include "Writer.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <math.h>
#include <sstream>
#include <utility>

using namespace Napi;
using namespace orc;
using std::cout;
using std::endl;
using std::find;
using std::ifstream;
using std::make_unique;
using std::move;
using std::pair;
using std::string;
using std::stringstream;
using std::vector;

namespace norc {
FunctionReference Writer::constructor; // NOLINT

void
Writer::Initialize(Napi::Env& env, Napi::Object& target)
{
  HandleScope scope(env);
  auto ctor = DefineClass(
    env,
    "Writer",
    { InstanceMethod("stringTypeSchema", &norc::Writer::StringTypeSchema),
      InstanceMethod("close", &norc::Writer::Close),
      InstanceMethod("schema", &norc::Writer::Schema),
      InstanceMethod("add", &norc::Writer::Add) });
  constructor = Napi::Persistent(ctor);
  constructor.SuppressDestruct();
  target.Set("Writer", ctor);
}

Writer::Writer(const CallbackInfo& info)
  : ObjectWrap(info)
{
  if (info.Length() < 1 || !info[0].IsString()) {
    Error::New(info.Env(), "Writer constructor requires an output location")
      .ThrowAsJavaScriptException();
  }
  output = writeLocalFile(info[0].As<String>());
  buffer = make_unique<DataBuffer<char>>(*getDefaultPool(), 4 * 1024 * 1024);
  options.setStripeSize((128 << 20));
  options.setCompressionBlockSize((64 << 10));
  options.setCompression(CompressionKind_ZLIB);
}
void
Writer::StringTypeSchema(const CallbackInfo& info)
{
  if (info.Length() < 1 || !info[0].IsString()) {
    TypeError::New(info.Env(), "Schema should be formatted as a string")
      .ThrowAsJavaScriptException();
  }
  string schema = info[0].As<String>();
  type = Type::buildTypeFromString(schema);
  writer = createWriter(*type, output.get(), options);
}

void
Writer::Schema(const CallbackInfo& info)
{
  if (info.Length() < 1 || !info[0].IsObject()) {
    TypeError::New(info.Env(), "The schema as an Object format is required")
      .ThrowAsJavaScriptException();
  }
  vector<string> types = { "boolean", "byte",      "short",   "int",
                           "long",    "float",     "double",  "string",
                           "binary",  "timestamp", "list",    "map",
                           "struct",  "union",     "decimal", "date",
                           "varchar", "char" };
  stringstream typeStr;
  typeStr << "struct<";
  auto schema = info[0].As<Object>();
  auto keys = schema.GetPropertyNames();
  for (uint32_t i = 0; i < keys.Length(); ++i) {
    string k = keys.Get(i).As<String>();
    TypeKind v = static_cast<TypeKind>(schema.Get(k).As<Number>().Int32Value());
    this->schema.emplace_back(pair<string, TypeKind>(k, v));
    if (find(types.begin(), types.end(), types[v]) == types.end()) {
      Error::New(info.Env(), "Invalid type: " + types[v] + " for key: " + k)
        .ThrowAsJavaScriptException();
      break;
    }
    typeStr << k << ":" << types[v];
    if (i != keys.Length()-1)
      typeStr << ",";
  }
  typeStr << ">";

  cout << "Setting File Schema as: " << typeStr.str() << endl;

  type = Type::buildTypeFromString(typeStr.str());
  writer = createWriter(*type, output.get(), options);
  batch = writer->createRowBatch(batchSize);
}

void
Writer::Add(const CallbackInfo& info)
{
  try {
    string col = info[0].As<String>();
    auto values = info[1].As<Array>();
    if (rows == 0) {
      rows = values.Length();
    } else if (rows != values.Length()) {
      Error::New(
        info.Env(),
        "Row count has already been defined by a previous call to Add. "
        "All rows must be of equal length row: " +
          std::to_string(rows) + " values: " + std::to_string(values.Length()))
        .ThrowAsJavaScriptException();
    }
    int structIdx = -1;
    for (int i = 0; i < schema.size(); i++) {
      if (schema[i].first == col) {
        structIdx = i;
        break;
      }
    }
    if (structIdx == -1) {
      Error::New(info.Env(), col + " not found").ThrowAsJavaScriptException();
      return;
    }
    switch (schema[structIdx].second) {
      case BYTE:
      case INT:
      case SHORT:
      case LONG: {
        AddLongColumn(values, structIdx);
        break;
      }
      case VARCHAR:
      case CHAR:
      case STRING:
      case BINARY: {
        AddStringColumn(values, structIdx);
        break;
      }

      case BOOLEAN: {
        AddBoolColumn(values, structIdx);
        break;
      }
      case FLOAT:
      case DOUBLE: {
        AddDoubleColumn(values, structIdx);
        break;
      }
      case TIMESTAMP: {
        AddTimestampColumn(values, structIdx);
        break;
      }
      case DECIMAL: {
        AddDecimalColumn(
          values,
          type->getSubtype(static_cast<uint64_t>(structIdx))->getScale(),
          type->getSubtype(static_cast<uint64_t>(structIdx))->getPrecision(),
          structIdx);
        break;
      }
      case DATE: {
        AddDateColumn(values, structIdx);
        break;
      }
      case LIST:
      case MAP:
      case STRUCT:
      case UNION: {
        Error::New(
          info.Env(),
          "List, Map, Struct, and Union types are not currently supported")
          .ThrowAsJavaScriptException();
        break;
      }
    }
  } catch (...) {
    Error::New(info.Env(), "Unable to add values to file")
      .ThrowAsJavaScriptException();
  }
}
void
Writer::AddLongColumn(const Array& data, int structIndex)
{
  auto row = dynamic_cast<StructVectorBatch*>(batch.get());
  auto batchCount = ceil(rows / batchSize);
  int arrayIdx = 0;
  for (int i = 0; i <= batchCount; i++) {
    auto longBatch = dynamic_cast<LongVectorBatch*>(row->fields[structIndex]);
    auto hasNull = false;
    uint64_t batchItemCount = 0;
    for (int j = 0; j < batchSize; j++) {
      if (arrayIdx == data.Length()) {
        break;
      }
      if (data[arrayIdx].IsNull()) {
        longBatch->notNull[j] = 0;
        hasNull = true;
      } else {
        longBatch->notNull[j] = 1;
        int64_t v =  data[arrayIdx].As<Number>();
        longBatch->data[j] = v;
      }
      batchItemCount++;
      arrayIdx++;
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = batchItemCount;
    row->numElements = batchItemCount;
    writer->add(*batch);
  }
}
void
Writer::AddStringColumn(const Array& data, int structIndex)
{

  auto row = dynamic_cast<StructVectorBatch*>(batch.get());
  auto batchCount = ceil(rows / batchSize);
  int arrayIdx = 0;
  for (int i = 0; i <= batchCount; i++) {
    auto stringBatch =
      dynamic_cast<StringVectorBatch*>(row->fields[structIndex]);
    auto hasNull = false;
    uint64_t batchItemCount = 0;
    for (int j = 0; j < batchSize; j++) {
      if (arrayIdx == data.Length()) {
        break;
      }
      string v = data[arrayIdx].As<String>();
      if (v.empty()) {
        batch->notNull[j] = 0;
        hasNull = true;
      } else {
        batch->notNull[j] = 1;
        if (buffer->size() - bufferOffset < v.size()) {
          buffer->reserve(buffer->size() * 2);
        }
        memcpy(buffer->data() + bufferOffset, v.c_str(), v.size());
        stringBatch->data[j] = buffer->data() + bufferOffset;
        stringBatch->length[j] = static_cast<uint64_t>(v.size());
        bufferOffset += v.size();
      }
      batchItemCount++;
      arrayIdx++;
    }
    stringBatch->hasNulls = hasNull;
    stringBatch->numElements = batchItemCount;
    row->numElements = batchItemCount;
    writer->add(*batch);
  }
}
void
Writer::AddDoubleColumn(const Array& data, int structIndex)
{
  auto row = dynamic_cast<StructVectorBatch*>(batch.get());
  auto batchCount = ceil(rows / batchSize);
  int arrayIdx = 0;
  for (int i = 0; i <= batchCount; i++) {
    auto dblBatch = dynamic_cast<DoubleVectorBatch*>(row->fields[structIndex]);
    auto hasNull = false;
    uint64_t batchItemCount = 0;
    for (int j = 0; j < batchSize; j++) {
      if (arrayIdx == data.Length()) {
        break;
      }
      if (data[arrayIdx].IsNull()) {
        batch->notNull[j] = 0;
        hasNull = true;
      } else {
        batch->notNull[j] = 1;
        dblBatch->data[j] = data[arrayIdx].As<Number>().FloatValue();
      }
      batchItemCount++;
      arrayIdx++;
    }
    dblBatch->hasNulls = hasNull;
    dblBatch->numElements = batchItemCount;
    row->numElements = batchItemCount;
    writer->add(*batch);
  }
}

// parse fixed point decimal numbers
void
Writer::AddDecimalColumn(const Array& data,
                         uint64_t scale,
                         uint64_t precision,
                         int structIndex)
{

  auto row = dynamic_cast<StructVectorBatch*>(batch.get());
  auto batchCount = ceil(rows / batchSize);
  int arrayIdx = 0;

  for (int i = 0; i <= batchCount; i++) {
    Decimal128VectorBatch* d128Batch = nullptr;
    Decimal64VectorBatch* d64Batch = nullptr;
    if (precision <= 18) {
      d64Batch = dynamic_cast<Decimal64VectorBatch*>(row->fields[structIndex]);
      d64Batch->scale = static_cast<int32_t>(scale);
    } else {
      d128Batch =
        dynamic_cast<Decimal128VectorBatch*>(row->fields[structIndex]);
      d128Batch->scale = static_cast<int32_t>(scale);
    }
    auto hasNull = false;

    uint64_t batchItemCount = 0;
    for (int j = 0; j < batchSize; j++) {
      if (arrayIdx == data.Length()) {
        break;
      }
      if (data[arrayIdx].IsNull()) {
        batch->notNull[j] = 0;
        hasNull = true;
      } else {
        batch->notNull[j] = 1;
        auto v = data[arrayIdx].As<Number>().Uint32Value();
        Int128 decimal(v);
        if (precision <= 18) {
          d64Batch->values[j] = decimal.toLong();
        } else {
          d128Batch->values[j] = decimal;
        }
      }
      batchItemCount++;
      arrayIdx++;
    }
    batch->hasNulls = hasNull;
    batch->numElements = batchItemCount;
    row->numElements = batchItemCount;
    writer->add(*batch);
  }
}

void
Writer::AddBoolColumn(const Array& data, int structIndex)
{
  auto row = dynamic_cast<StructVectorBatch*>(batch.get());
  auto batchCount = ceil(rows / batchSize);
  int arrayIdx = 0;
  for (int i = 0; i <= batchCount; i++) {
    auto boolBatch = dynamic_cast<LongVectorBatch*>(row->fields[structIndex]);
    auto hasNull = false;
    uint64_t batchItemCount = 0;
    for (int j = 0; j < batchSize; j++) {
      if (arrayIdx == data.Length()) {
        break;
      }
      if (data[arrayIdx].IsNull()) {
        batch->notNull[j] = 0;
        hasNull = true;
      } else {
        batch->notNull[j] = 1;
        boolBatch->data[j] = data[arrayIdx].As<Boolean>();
      }

      batchItemCount++;
      arrayIdx++;
    }
    boolBatch->hasNulls = hasNull;
    boolBatch->numElements = batchItemCount;
    row->numElements = batchItemCount;
    writer->add(*batch);
  }
}

// parse date string from format YYYY-mm-dd
void
Writer::AddDateColumn(const Array& data, int structIndex)
{
  auto row = dynamic_cast<StructVectorBatch*>(batch.get());
  auto batchCount = ceil(rows / batchSize);
  int arrayIdx = 0;
  for (int i = 0; i <= batchCount; i++) {
    auto timeBatch = dynamic_cast<LongVectorBatch*>(row->fields[structIndex]);
    auto hasNull = false;
    uint64_t batchItemCount = 0;
    for (int j = 0; j < batchSize; j++) {
      if (arrayIdx == data.Length()) {
        break;
      }
      if (data[arrayIdx].IsNull()) {
        batch->notNull[j] = 0;
        hasNull = true;
      } else {
        string v = data[arrayIdx].As<String>();
        batch->notNull[j] = 1;
        struct tm tm
        {};
        memset(&tm, 0, sizeof(struct tm));
        strptime(v.c_str(), "%Y-%m-%d", &tm);
        time_t t = mktime(&tm);
        time_t t1970 = 0;
        double seconds = difftime(t, t1970);
        auto days = static_cast<int64_t>(seconds / (60 * 60 * 24));
        timeBatch->data[j] = days;
      }
      batchItemCount++;
      arrayIdx++;
    }
    timeBatch->hasNulls = hasNull;
    timeBatch->numElements = batchItemCount;
    row->numElements = batchItemCount;
    writer->add(*batch);
  }
}
void
Writer::AddTimestampColumn(const Array& data, int structIndex)
{
  auto row = dynamic_cast<StructVectorBatch*>(batch.get());
  auto batchCount = ceil(rows / batchSize);
  int arrayIdx = 0;
  for (int i = 0; i <= batchCount; i++) {
    auto tsBatch =
      dynamic_cast<TimestampVectorBatch*>(row->fields[structIndex]);
    auto hasNull = false;
    uint64_t batchItemCount = 0;
    for (int j = 0; j < batchSize; j++) {
      if (arrayIdx == data.Length()) {
        break;
      }
      struct tm timeStruct
      {};
      if (data[arrayIdx].IsNull()) {
        batch->notNull[j] = 0;
        hasNull = true;
      } else {
        string v = data[arrayIdx].As<String>();
        memset(&timeStruct, 0, sizeof(timeStruct));
        char* left = strptime(v.c_str(), "%Y-%m-%d %H:%M:%S", &timeStruct);
        if (left == nullptr) {
          batch->notNull[j] = 0;
        } else {
          batch->notNull[j] = 1;
          tsBatch->data[j] = timegm(&timeStruct);
          char* tail;
          double d = strtod(left, &tail);
          if (tail != left) {
            tsBatch->nanoseconds[j] = static_cast<long>(d * 1000000000.0);
          } else {
            tsBatch->nanoseconds[j] = 0;
          }
        }
      }
      batchItemCount++;
      arrayIdx++;
    }
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = batchItemCount;
    row->numElements = batchItemCount;
    writer->add(*batch);
  }
}
void
Writer::Close(const CallbackInfo&)
{
  writer->close();
}

// class ImportCSVWorker : public AsyncWorker
//{
// public:
//  ImportCSVWorker(Function& cb, norc::Writer& self, string csv)
//    : AsyncWorker(cb)
//    , writer(self)
//    , csv(std::move(csv))
//  {}
//
// private:
//  Writer& writer;
//  string csv;
//
//  string columnString(const string& v, uint64_t idx)
//  {
//    uint64_t col = 0;
//    size_t start = 0;
//    size_t end = v.find(',');
//    while (col < idx && end != string::npos) {
//      start = end + 1;
//      end = v.find(',', start);
//      ++col;
//    }
//    return col == idx ? v.substr(start, end - start) : "";
//  }
//
// protected:
//  void Execute() override
//  {
//    ifstream source(csv.c_str());
//    if (!source.good()) {
//      SetError("Unable to open/read csv file");
//      return;
//    }
//    bool eof = false;
//    string line;
//    vector<string> data;
//    unique_ptr<ColumnVectorBatch> row = writer.writer->createRowBatch(1024);
//    DataBuffer<char> buffer(*getDefaultPool(), 4 * 1024 * 1024);
//    while (!eof) {
//      uint64_t valuesRead = 0;
//      uint64_t bufferOffset = 0;
//      data.clear();
//      memset(row->notNull.data(), 1, 1024);
//      for (uint64_t i = 0; i < 1024; i++) {
//        if (!std::getline(source, line)) {
//          eof = true;
//          break;
//        }
//        data.emplace_back(line);
//        ++valuesRead;
//      }
//      if (valuesRead > 0) {
//        auto batch = dynamic_cast<StructVectorBatch*>(row.get());
//        batch->numElements = valuesRead;
//        for (uint64_t i = 0; i < batch->fields.size(); i++) {
//          auto subType = writer.type->getSubtype(i);
//          switch (subType->getKind()) {
//            case BYTE:
//            case INT:
//            case SHORT:
//            case LONG:
//              setLongTypeValue(data, batch->fields[i], valuesRead, i);
//              break;
//            case STRING:
//            case VARCHAR:
//            case CHAR:
//            case BINARY:
//              setStringTypeValue(
//                data, batch->fields[i], valuesRead, i, buffer, bufferOffset);
//              break;
//
//            case FLOAT:
//            case DOUBLE:
//              setDoubleTypeValues(data, batch->fields[i], valuesRead, i);
//              break;
//
//            case BOOLEAN:
//              setBoolTypeValue(data, batch->fields[i], valuesRead, i);
//              break;
//
//            case DECIMAL:
//              setDecimalTypeValue(data,
//                                  batch->fields[i],
//                                  valuesRead,
//                                  i,
//                                  subType->getScale(),
//                                  subType->getPrecision());
//              break;
//
//            case TIMESTAMP:
//              setTimestampTypeValue(data, batch->fields[i], valuesRead, i);
//              break;
//            case DATE:
//              setDateTypeValue(data, batch->fields[i], valuesRead, i);
//              break;
//
//            case LIST:
//            case MAP:
//            case STRUCT:
//            case UNION:
//              SetError(subType->toString() + " is not yet supported");
//              break;
//          }
//        }
//        writer.writer->add(*row);
//      }
//    }
//  }
//  void OnOK() override
//  {
//    HandleScope scope(Env());
//    Callback().Call({ Env().Undefined(), writer.Value() });
//  }
//};
// void
// Writer::ImportCSV(const CallbackInfo& info)
//{
//  if (!writer) {
//    Error::New(info.Env(), "A schema must be defined before importing csv
//    data")
//      .ThrowAsJavaScriptException();
//  }
//  if (info.Length() < 2 || !info[0].IsString() || !info[1].IsFunction()) {
//    Error::New(info.Env(), "File path and callback are required")
//      .ThrowAsJavaScriptException();
//  }
//  auto cb = info[1].As<Function>();
//  auto worker = new ImportCSVWorker(cb, *this, info[0].As<String>());
//  worker->Queue();
//}
}
