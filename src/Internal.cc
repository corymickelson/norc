//
// Created by developer on 11/6/18.
//

#include "Internal.h"
#include "../include/json.hpp"
#include <orc/OrcFile.hh>

using namespace Napi;
using namespace orc;
using json = nlohmann::json;
using std::string;

namespace norc {
void
LineToJSON(Napi::Env env, std::string& line, Napi::Object& target)
{
  json j = json::parse(line);
  for (json::iterator it = j.begin(); it != j.end(); ++it) {
    const string& k = it.key();
    if (it.value().is_boolean()) {
      target.Set(k, Boolean::New(env, it.value()));
    } else if (it.value().is_number() || it.value().is_number_float() ||
               it.value().is_number_integer() ||
               it.value().is_number_unsigned()) {
      target.Set(k, Number::New(env, it.value()));
    } else if (it.value().is_string()) {
      string v = it.value();
      target.Set(k, String::New(env, v));
    } else if (it.value().is_null()) {
      target.Set(k, env.Null());
    } else {
      target.Set(k,
                 Error::New(env,
                            string("Unable to convert ")
                              .append(k)
                              .append(" from string: ")
                              .append(line))
                   .Value());
    }
  }
}
void
AddNumberType(Napi::Env env,
              orc::ColumnVectorBatch* batch,
              uint64_t offset,
              Napi::Value value)
{
  auto longBatch = dynamic_cast<LongVectorBatch*>(batch);
  if (value.IsNull()) {
    batch->notNull[offset] = 0;
    longBatch->hasNulls = true;
  } else {
    batch->notNull[offset] = 1;
    longBatch->data[offset] = value.As<Number>().Uint32Value();
    longBatch->numElements = offset;
  }
}
void
AddStringType(Napi::Env env,
              orc::ColumnVectorBatch* batch,
              orc::DataBuffer<char>* buffer,
              uint64_t batchOffset,
              uint64_t* bufferOffset,
              Napi::Value value)
{
  auto stringBatch = dynamic_cast<StringVectorBatch*>(batch);
  if (value.IsNull()) {
    batch->notNull[batchOffset] = 0;
    stringBatch->hasNulls = true;
  } else {
    string v = value.As<String>();
    batch->notNull[batchOffset] = 1;
    if (buffer->size() - *bufferOffset < v.size()) {
      buffer->reserve(buffer->size() * 2);
    }
    memcpy(buffer->data() + *bufferOffset, v.c_str(), v.size());
    stringBatch->data[batchOffset] = buffer->data() + *bufferOffset;
    stringBatch->length[batchOffset] = static_cast<long>(v.size());
    *bufferOffset += v.size();
  }
  stringBatch->numElements = batchOffset;
}
void
AddBoolType(Napi::Env env,
            orc::ColumnVectorBatch* batch,
            uint64_t batchOffset,
            Napi::Value value)
{
  auto boolBatch = dynamic_cast<LongVectorBatch*>(batch);
  if (value.IsNull()) {
    batch->notNull[batchOffset] = 0;
    boolBatch->hasNulls = true;
  } else {
    batch->notNull[batchOffset] = 1;
    boolBatch->data[batchOffset] = value.As<Boolean>();
  }
  boolBatch->numElements = batchOffset;
}
void
AddFloatType(Napi::Env env,
             orc::ColumnVectorBatch* batch,
             uint64_t batchOffset,
             Napi::Value value)
{
  auto dblBatch = dynamic_cast<DoubleVectorBatch*>(batch);
  if (value.IsNull()) {
    batch->notNull[batchOffset] = 0;
    dblBatch->hasNulls = true;
  } else {
    batch->notNull[batchOffset] = 1;
    dblBatch->data[batchOffset] = value.As<Number>().DoubleValue();
  }
  dblBatch->numElements = batchOffset;
}
void
AddTimeType(Napi::Env env,
            orc::ColumnVectorBatch* batch,
            uint64_t batchOffset,
            Napi::Value value)
{
  auto tsBatch = dynamic_cast<TimestampVectorBatch*>(batch);
  struct tm timeStruct
  {};
  if (value.IsNull()) {
    batch->notNull[batchOffset] = 0;
    tsBatch->hasNulls = true;
  } else {
    string v = value.As<String>();
    memset(&timeStruct, 0, sizeof(timeStruct));
    char* left = strptime(v.c_str(), "%Y-%m-%d %H:%M:%S", &timeStruct);
    if (left == nullptr) {
      batch->notNull[batchOffset] = 0;
    } else {
      batch->notNull[batchOffset] = 1;
      tsBatch->data[batchOffset] = timegm(&timeStruct);
      char* tail;
      double d = strtod(left, &tail);
      if (tail != left) {
        tsBatch->nanoseconds[batchOffset] = static_cast<long>(d * 1000000000.0);
      } else {
        tsBatch->nanoseconds[batchOffset] = 0;
      }
    }
  }
  tsBatch->numElements = batchOffset;
}
void
AddDecimalType(Napi::Env env,
               orc::ColumnVectorBatch* batch,
               orc::Type* type,
               uint64_t batchOffset,
               int idx,
               Napi::Value value)
{
  auto precision = type->getPrecision();
  auto scale = type->getScale();
  orc::Decimal128VectorBatch* d128Batch = ORC_NULLPTR;
  orc::Decimal64VectorBatch* d64Batch = ORC_NULLPTR;
  if (precision <= 18) {
    d64Batch = dynamic_cast<orc::Decimal64VectorBatch*>(batch);
    d64Batch->scale = static_cast<int32_t>(scale);
  } else {
    d128Batch = dynamic_cast<orc::Decimal128VectorBatch*>(batch);
    d128Batch->scale = static_cast<int32_t>(scale);
  }
  if (value.IsNull()) {
    batch->notNull[batchOffset] = 0;
    if (precision <= 18) {
      d64Batch->hasNulls = true;
    } else {
      d128Batch->hasNulls = true;
    }

  } else {
    auto v = value.As<Number>().Uint32Value();
    Int128 decimal(v);
    batch->notNull[batchOffset] = 1;

    if (precision <= 18) {
      d64Batch->values[idx] = decimal.toLong();
    } else {
      d128Batch->values[idx] = decimal;
    }
  }
  d64Batch->numElements = batchOffset;
}
void
AddDateType(Napi::Env env,
            orc::ColumnVectorBatch* batch,
            uint64_t batchOffset,
            Napi::Value value)
{
  auto timeBatch = dynamic_cast<LongVectorBatch*>(batch);
  if (value.IsNull()) {
    batch->notNull[batchOffset] = 0;
    timeBatch->hasNulls = true;
  } else {
    string v = value.As<String>();
    batch->notNull[batchOffset] = 1;
    struct tm tm
    {};
    memset(&tm, 0, sizeof(struct tm));
    strptime(v.c_str(), "%Y-%m-%d", &tm);
    time_t t = mktime(&tm);
    time_t t1970 = 0;
    double seconds = difftime(t, t1970);
    auto days = static_cast<int64_t>(seconds / (60 * 60 * 24));
    timeBatch->data[batchOffset] = days;
  }
  timeBatch->numElements = batchOffset;
}
}
