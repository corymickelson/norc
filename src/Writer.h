//
// Created by developer on 9/12/18.
//

#ifndef NORC_WRITER_H
#define NORC_WRITER_H

#include <napi.h>
#include <orc/OrcFile.hh>

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
  unique_ptr<orc::OutputStream> output;
  unique_ptr<orc::Writer> writer;
  unique_ptr<orc::Type> type;
  orc::WriterOptions options;
};
}
#endif // NORC_WRITER_H
