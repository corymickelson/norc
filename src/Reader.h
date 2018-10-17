/**
 * This file is part of the norc (R) project.
 * Copyright (c) 2017-2018
 * Authors: Cory Mickelson, et al.
 *
 * norc is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * norc is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
  uint64_t chunkSize = 8 << 10;
};
}

#endif // NORC_READER_H
