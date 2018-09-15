
//
// Created by developer on 9/12/18.
//

#include "Writer.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>

using namespace Napi;
using namespace orc;
using std::cout;
using std::endl;
using std::find;
using std::ifstream;
using std::string;
using std::stringstream;
using std::vector;
using std::map;
using std::pair;

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
      InstanceMethod("importCSV", &norc::Writer::ImportCSV) });
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

Napi::Value
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
  for (uint32_t i = 0; i < keys.Length(); i++) {
    string k = keys.Get(i).As<String>();
    TypeKind v = static_cast<TypeKind>(schema.Get(k).As<Number>().Int32Value());
    schemaMap.insert(pair<string, TypeKind>(k, v));
    if (find(types.begin(), types.end(), types[v]) == types.end()) {
      Error::New(info.Env(), "Invalid type: " + types[v] + " for key: " + k)
        .ThrowAsJavaScriptException();
      break;
    }
    typeStr << k << ":" << v;
    if (i < keys.Length())
      typeStr << ",";
  }
  typeStr << ">";

  cout << "Setting File Schema as: " << typeStr.str() << endl;

  type = Type::buildTypeFromString(typeStr.str());
  writer = createWriter(*type, output.get(), options);
  batch = writer->createRowBatch(batchSize);
}

void
Writer::Add(const CallbackInfo&)
{}

void
Writer::Close(const CallbackInfo&)
{
  writer->close();
}

class ImportCSVWorker : public AsyncWorker
{
public:
  ImportCSVWorker(Function& cb, norc::Writer& self, string csv)
    : AsyncWorker(cb)
    , writer(self)
    , csv(std::move(csv))
  {}

private:
  Writer& writer;
  string csv;

  string columnString(const string& v, uint64_t idx)
  {
    uint64_t col = 0;
    size_t start = 0;
    size_t end = v.find(',');
    while (col < idx && end != string::npos) {
      start = end + 1;
      end = v.find(',', start);
      ++col;
    }
    return col == idx ? v.substr(start, end - start) : "";
  }
  void setLongTypeValue(const vector<string>& data,
                        ColumnVectorBatch* batch,
                        uint64_t valuesRead,
                        uint64_t idx)
  {
    auto longBatch = dynamic_cast<LongVectorBatch*>(batch);
    auto hasNull = false;
    for (uint64_t i = 0; i < valuesRead; i++) {
      string csvCol = columnString(data[i], idx);
      if (csvCol.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        longBatch->data[i] = atoll(csvCol.c_str());
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = valuesRead;
  }

  void setStringTypeValue(const vector<string>& data,
                          ColumnVectorBatch* batch,
                          uint64_t valuesRead,
                          uint64_t idx,
                          DataBuffer<char>& buffer,
                          uint64_t& offset)
  {
    auto stringBatch = dynamic_cast<StringVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < valuesRead; i++) {
      auto csvCol = columnString(data[i], idx);
      if (csvCol.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        if (buffer.size() - offset < csvCol.size()) {
          buffer.reserve(buffer.size() * 2);
        }
        memcpy(buffer.data() + offset, csvCol.c_str(), csvCol.size());
        stringBatch->data[i] = buffer.data() + offset;
        stringBatch->length[i] = static_cast<uint64_t>(csvCol.size());
        offset += csvCol.size();
      }
    }
    stringBatch->hasNulls = hasNull;
    stringBatch->numElements = valuesRead;
  }
  void setDoubleTypeValues(const vector<string>& data,
                           ColumnVectorBatch* batch,
                           uint64_t numValues,
                           uint64_t colIndex)
  {
    auto dblBatch = dynamic_cast<DoubleVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = columnString(data[i], colIndex);
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        dblBatch->data[i] = atof(col.c_str());
      }
    }
    dblBatch->hasNulls = hasNull;
    dblBatch->numElements = numValues;
  }

  // parse fixed point decimal numbers
  void setDecimalTypeValue(const vector<string>& data,
                           ColumnVectorBatch* batch,
                           uint64_t numValues,
                           uint64_t colIndex,
                           size_t scale,
                           size_t precision)
  {

    Decimal128VectorBatch* d128Batch = nullptr;
    Decimal64VectorBatch* d64Batch = nullptr;
    if (precision <= 18) {
      d64Batch = dynamic_cast<Decimal64VectorBatch*>(batch);
      d64Batch->scale = static_cast<int32_t>(scale);
    } else {
      d128Batch = dynamic_cast<Decimal128VectorBatch*>(batch);
      d128Batch->scale = static_cast<int32_t>(scale);
    }
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      string col = columnString(data[i], colIndex);
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        size_t ptPos = col.find('.');
        size_t curScale = 0;
        string num = col;
        if (ptPos != string::npos) {
          curScale = col.length() - ptPos - 1;
          num = col.substr(0, ptPos) + col.substr(ptPos + 1);
        }
        Int128 decimal(num);
        while (curScale != scale) {
          curScale++;
          decimal *= 10;
        }
        if (precision <= 18) {
          d64Batch->values[i] = decimal.toLong();
        } else {
          d128Batch->values[i] = decimal;
        }
      }
    }
    batch->hasNulls = hasNull;
    batch->numElements = numValues;
  }

  void setBoolTypeValue(const vector<string>& data,
                        ColumnVectorBatch* batch,
                        uint64_t numValues,
                        uint64_t colIndex)
  {
    auto boolBatch = dynamic_cast<LongVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      string col = columnString(data[i], colIndex);
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        std::transform(col.begin(), col.end(), col.begin(), ::tolower);
        if (col == "true" || col == "t") {
          boolBatch->data[i] = true;
        } else {
          boolBatch->data[i] = false;
        }
      }
    }
    boolBatch->hasNulls = hasNull;
    boolBatch->numElements = numValues;
  }

  // parse date string from format YYYY-mm-dd
  void setDateTypeValue(const std::vector<std::string>& data,
                        orc::ColumnVectorBatch* batch,
                        uint64_t numValues,
                        uint64_t colIndex)
  {
    auto* longBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = columnString(data[i], colIndex);
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        struct tm tm
        {};
        memset(&tm, 0, sizeof(struct tm));
        strptime(col.c_str(), "%Y-%m-%d", &tm);
        time_t t = mktime(&tm);
        time_t t1970 = 0;
        double seconds = difftime(t, t1970);
        auto days = static_cast<int64_t>(seconds / (60 * 60 * 24));
        longBatch->data[i] = days;
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
  }
  void setTimestampTypeValue(const std::vector<std::string>& data,
                             orc::ColumnVectorBatch* batch,
                             uint64_t numValues,
                             uint64_t colIndex)
  {
    struct tm timeStruct
    {};
    auto* tsBatch = dynamic_cast<orc::TimestampVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = columnString(data[i], colIndex);
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        memset(&timeStruct, 0, sizeof(timeStruct));
        char* left = strptime(col.c_str(), "%Y-%m-%d %H:%M:%S", &timeStruct);
        if (left == nullptr) {
          batch->notNull[i] = 0;
        } else {
          batch->notNull[i] = 1;
          tsBatch->data[i] = timegm(&timeStruct);
          char* tail;
          double d = strtod(left, &tail);
          if (tail != left) {
            tsBatch->nanoseconds[i] = static_cast<long>(d * 1000000000.0);
          } else {
            tsBatch->nanoseconds[i] = 0;
          }
        }
      }
    }
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = numValues;
  }

protected:
  void Execute() override
  {
    ifstream source(csv.c_str());
    if (!source.good()) {
      SetError("Unable to open/read csv file");
      return;
    }
    bool eof = false;
    string line;
    vector<string> data;
    unique_ptr<ColumnVectorBatch> row = writer.writer->createRowBatch(1024);
    DataBuffer<char> buffer(*getDefaultPool(), 4 * 1024 * 1024);
    while (!eof) {
      uint64_t valuesRead = 0;
      uint64_t bufferOffset = 0;
      data.clear();
      memset(row->notNull.data(), 1, 1024);
      for (uint64_t i = 0; i < 1024; i++) {
        if (!std::getline(source, line)) {
          eof = true;
          break;
        }
        data.emplace_back(line);
        ++valuesRead;
      }
      if (valuesRead > 0) {
        auto batch = dynamic_cast<StructVectorBatch*>(row.get());
        batch->numElements = valuesRead;
        for (uint64_t i = 0; i < batch->fields.size(); i++) {
          auto subType = writer.type->getSubtype(i);
          switch (subType->getKind()) {
            case BYTE:
            case INT:
            case SHORT:
            case LONG:
              setLongTypeValue(data, batch->fields[i], valuesRead, i);
              break;
            case STRING:
            case VARCHAR:
            case CHAR:
            case BINARY:
              setStringTypeValue(
                data, batch->fields[i], valuesRead, i, buffer, bufferOffset);
              break;

            case FLOAT:
            case DOUBLE:
              setDoubleTypeValues(data, batch->fields[i], valuesRead, i);
              break;

            case BOOLEAN:
              setBoolTypeValue(data, batch->fields[i], valuesRead, i);
              break;

            case DECIMAL:
              setDecimalTypeValue(data,
                                  batch->fields[i],
                                  valuesRead,
                                  i,
                                  subType->getScale(),
                                  subType->getPrecision());
              break;

            case TIMESTAMP:
              setTimestampTypeValue(data, batch->fields[i], valuesRead, i);
              break;
            case DATE:
              setDateTypeValue(data, batch->fields[i], valuesRead, i);
              break;

            case LIST:
            case MAP:
            case STRUCT:
            case UNION:
              SetError(subType->toString() + " is not yet supported");
              break;
          }
        }
        writer.writer->add(*row);
      }
    }
  }
  void OnOK() override
  {
    HandleScope scope(Env());
    Callback().Call({ Env().Undefined(), writer.Value() });
  }
};
void
Writer::ImportCSV(const CallbackInfo& info)
{
  if (!writer) {
    Error::New(info.Env(), "A schema must be defined before importing csv data")
      .ThrowAsJavaScriptException();
  }
  if (info.Length() < 2 || !info[0].IsString() || !info[1].IsFunction()) {
    Error::New(info.Env(), "File path and callback are required")
      .ThrowAsJavaScriptException();
  }
  auto cb = info[1].As<Function>();
  auto worker = new ImportCSVWorker(cb, *this, info[0].As<String>());
  worker->Queue();
}
}
