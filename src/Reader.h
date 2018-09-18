//
// Created by developer on 9/16/18.
//

#ifndef NORC_READER_H
#define NORC_READER_H

#include <napi.h>
#include <orc/OrcFile.hh>

using Napi::CallbackInfo;
using std::unique_ptr;
using std::vector;
using std::string;

namespace norc {

struct NorcColumnMetadata {
public:
  string title;
  unsigned int index;
  orc::TypeKind type;
};

class Reader : public Napi::ObjectWrap<Reader>
{
public:
  static Napi::FunctionReference constructor;
  static void Initialize(Napi::Env&, Napi::Object&);
  explicit Reader(const CallbackInfo&);
  ~Reader();
  void Read(const CallbackInfo&);
  Napi::Value GetColumnStatistics(const CallbackInfo&);
  Napi::Value GetWriterVersion(const CallbackInfo&);
  Napi::Value GetFormatVersion(const CallbackInfo&);
  Napi::Value GetCompression(const CallbackInfo&);
  Napi::Value GetType(const CallbackInfo&);
  orc::ReaderOptions readerOptions;
  unique_ptr<orc::Reader> reader;
  vector<NorcColumnMetadata> fileMeta;
  Napi::Buffer<string> data;
  uint64_t chunkSize = 4096;
};
}

#endif // NORC_READER_H
