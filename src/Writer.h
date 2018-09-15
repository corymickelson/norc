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

namespace norc {

class Writer : public Napi::ObjectWrap<Writer>
{
public:
  static Napi::FunctionReference constructor;
  static void Initialize(Napi::Env&, Napi::Object&);
  explicit Writer(const CallbackInfo&);
  void Close(const CallbackInfo&);
  void StringTypeSchema(const CallbackInfo&);
  void ImportCSV(const CallbackInfo&);
  Napi::Value Schema(const CallbackInfo&);
  void Add(const CallbackInfo&);
  StructVectorBatch* GetBatchRoot()
  {
    if (!batch) {
      Napi::Error::New(Env(), "Schema must be defined first")
        .ThrowAsJavaScriptException();
      return nullptr;
    } else {
      return dynamic_cast<orc::StructVectorBatch*>(batch.get());
    }
  }
  unique_ptr<orc::OutputStream> output;
  unique_ptr<orc::Writer> writer;
  unique_ptr<orc::Type> type;
  unique_ptr<orc::ColumnVectorBatch> batch;
  orc::WriterOptions options;
  std::map<std::string, orc::TypeKind> schemaMap;
  uint64_t rows = 0;
  uint64_t batchSize = 1024
};
}
#endif // NORC_WRITER_H
