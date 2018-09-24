//
// Created by developer on 9/12/18.
//

#ifndef NORC_WRITER_H
#define NORC_WRITER_H

#include <map>
#include <napi.h>
#include <orc/OrcFile.hh>

using Napi::CallbackInfo;
using std::string;
using std::unique_ptr;
using std::vector;

namespace norc {
enum JsSchemaDataType
{
  BOOLEAN = 0,
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  STRING,
  BINARY,
  TIMESTAMP,
  ARRAY,
  MAP,
  STRUCT,
  UNION,
  DECIMAL,
  DATE,
  VARCHAR,
  CHAR
};
class Writer : public Napi::ObjectWrap<Writer>
{
public:
  static Napi::FunctionReference constructor;
  static void Initialize(Napi::Env&, Napi::Object&);
  explicit Writer(const CallbackInfo&);
  ~Writer();

  void Close(const CallbackInfo&);
  void StringTypeSchema(const CallbackInfo&);
  void ImportCSV(const CallbackInfo&);
  void Schema(const CallbackInfo&);
  void Add(const CallbackInfo&);
  void AddObject(const CallbackInfo&, Napi::Object);
  Napi::Value GetBuffer(const CallbackInfo&);

  unique_ptr<orc::OutputStream> output;
  unique_ptr<orc::Writer> writer;
  unique_ptr<orc::Type> type;
  unique_ptr<orc::ColumnVectorBatch> batch;
  orc::WriterOptions options;
  unique_ptr<orc::DataBuffer<char>> buffer;
  std::vector<std::pair<std::string, orc::TypeKind>> schema;
  uint64_t batchSize = 1024;
  uint64_t bufferOffset = 0;
  uint64_t batchOffset = 0;
};
}
#endif // NORC_WRITER_H
