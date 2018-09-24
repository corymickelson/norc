//
// Created by developer on 9/23/18.
//

#include "MemoryFile.h"

using std::unique_ptr;

namespace norc {

MemoryWriter::MemoryWriter(size_t bufSize, Napi::Env env)
  : env(env)
  , name("MemoryWriter")
{
  data = new char[bufSize];
  length = 0;
}
MemoryWriter::~MemoryWriter()
{
  delete[] data;
}
void
MemoryWriter::write(const void* line, size_t size)
{
  auto x = reinterpret_cast<const char*>(line);

  memcpy(data + size, line, size);
//  data +=  x;
  length += size;
}
void
MemoryWriter::close()
{
  buffer = Napi::Buffer<char>::New(env, data, length);
}
}