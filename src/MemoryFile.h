//
// Created by developer on 9/23/18.
//

#ifndef NORC_MEMORYFILE_H
#define NORC_MEMORYFILE_H

#include <orc/OrcFile.hh>
#include <napi.h>

using std::string;

namespace norc {

const size_t MEMORY_FILE_SIZE = 100 * 1024 * 1024;

class MemoryWriter: public orc::OutputStream
{
public:
  explicit MemoryWriter(size_t, Napi::Env);
  ~MemoryWriter() override;
  uint64_t getLength() const override { return length; }
  uint64_t getNaturalWriteSize() const override {return writeSize; }
  void write(const void*, size_t) override;
  const string& getName() const override {return name;}
  const char* getData() const { return data; }
  void close() override;
private:
  Napi::Env env;
  Napi::Buffer<char> buffer;
  char* data;
  uint64_t length;
  uint64_t writeSize;
  string name;
};

}

#endif //NORC_MEMORYFILE_H
