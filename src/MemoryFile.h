//
// Created by developer on 9/23/18.
//

#ifndef NORC_MEMORYFILE_H
#define NORC_MEMORYFILE_H

#include <napi.h>
#include <orc/OrcFile.hh>

using std::string;

namespace norc {

const size_t MEMORY_FILE_SIZE = 100 * 1024 * 1024;

class MemoryWriter : public orc::OutputStream
{
public:
  explicit MemoryWriter(size_t);
  ~MemoryWriter() override;
  uint64_t getLength() const override { return length; }
  uint64_t getNaturalWriteSize() const override { return writeSize; }
  void write(const void*, size_t) override;
  const string& getName() const override { return name; }
  char* getData() const { return data; }
  void close() override;

private:
  char* data;
  uint64_t length;
  uint64_t writeSize;
  string name;
  size_t alloc;
};

class MemoryReader : public orc::InputStream
{
public:
  MemoryReader(const char*, size_t);
  ~MemoryReader() override;
  uint64_t getLength() const override { return size; }
  uint64_t getNaturalReadSize() const override { return naturalSize; }
  void read(void* buf, uint64_t length, uint64_t offset) override;
  const std::string& getName() const override { return name; }
  const char* getData() const { return buffer; }

private:
  const char* buffer;
  uint64_t size, naturalSize;
  std::string name;
};

}

#endif // NORC_MEMORYFILE_H
