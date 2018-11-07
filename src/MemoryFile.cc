//
// Created by developer on 9/23/18.
//

#include "MemoryFile.h"

using std::cout;
using std::endl;
using std::unique_ptr;

namespace norc {

MemoryWriter::MemoryWriter(size_t bufSize)
  : name("MemoryWriter")
  , alloc(bufSize)
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
  if (length + size > alloc) {
    if (void* mem = realloc(data, alloc * 2)) {
      data = static_cast<char*>(mem);
      alloc *= 2;
    } else {
      throw std::bad_alloc();
    }
  }
  auto input = reinterpret_cast<const char*>(line);
  for (long i = 0; i < size; i++) {
    data[length] = input[i];
    length++;
  }
}
void
MemoryWriter::close()
{}

MemoryReader::MemoryReader(const char* buffer, size_t size)
  : buffer(buffer)
  , size(size)
  , name("MemoryReader")
  , naturalSize(1024)
{}
MemoryReader::~MemoryReader() = default;
void
MemoryReader::read(void *buf, uint64_t length, uint64_t offset)
{
  memcpy(buf, buffer + offset, length);
}
}