//
// Created by developer on 9/12/18.
//

#ifndef NORC_WRITER_H
#define NORC_WRITER_H

#include <napi.h>
#include <orc/OrcFile.hh>
#include <map>

using Napi::CallbackInfo;
using std::unique_ptr;
using std::vector;
using std::string;

namespace norc {

class Writer : public Napi::ObjectWrap<Writer>
{
public:
  static Napi::FunctionReference constructor;
  static void Initialize(Napi::Env&, Napi::Object&);
  explicit Writer(const CallbackInfo&);
  void Close(const CallbackInfo&);
  void StringTypeSchema(const CallbackInfo&);
//  void ImportCSV(const CallbackInfo&);
  void Schema(const CallbackInfo&);
  void Add(const CallbackInfo&);
  orc::StructVectorBatch* GetBatchRoot()
  {
    if (!batch) {
      Napi::Error::New(Env(), "Schema must be defined first")
        .ThrowAsJavaScriptException();
      return nullptr;
    } else {
      return dynamic_cast<orc::StructVectorBatch*>(batch.get());
    }
  }
  void AddLongColumn(const Napi::Array&, int);
  void AddStringColumn(const Napi::Array&, int);
  void AddDoubleColumn(const Napi::Array&, int);
  void AddDecimalColumn(const Napi::Array&, uint64_t scale, uint64_t precision, int structIndex);
  void AddBoolColumn(const Napi::Array&,int);
  void AddDateColumn(const Napi::Array&,int);
  void AddTimestampColumn(const Napi::Array&,int);


  unique_ptr<orc::OutputStream> output;
  unique_ptr<orc::Writer> writer;
  unique_ptr<orc::Type> type;
  unique_ptr<orc::ColumnVectorBatch> batch;
  orc::WriterOptions options;
  unique_ptr<orc::DataBuffer<char>> buffer;
  std::vector<std::pair<std::string, orc::TypeKind>> schema;
  uint64_t rows = 0;
  uint64_t batchSize = 1024;
  u_int64_t bufferOffset = 0;
};
}
#endif // NORC_WRITER_H
