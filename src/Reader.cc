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
#include "Reader.h"
#include "../include/json.hpp"
#include "ValidateArguments.h"
#include <algorithm>
#include <orc/ColumnPrinter.hh>

using namespace Napi;
using namespace orc;
using std::cout;
using std::endl;
using std::find;
using std::list;
using std::make_unique;
using std::next;
using std::pair;
using std::string;
using json = nlohmann::json;

namespace norc {
FunctionReference Reader::constructor; // NOLINT
void
Reader::Initialize(Napi::Env& env, Napi::Object& target)
{
  HandleScope scope(env);
  auto ctor = DefineClass(
    env,
    "Reader",
    { InstanceMethod("read", &Reader::Read),
      InstanceMethod("columnStatistics", &Reader::GetColumnStatistics),
      InstanceAccessor("writeVersion", &Reader::GetWriterVersion, nullptr),
      InstanceAccessor("formatVersion", &Reader::GetFormatVersion, nullptr),
      InstanceAccessor("compression", &Reader::GetCompression, nullptr),
      InstanceAccessor("type", &Reader::GetType, nullptr) });
  constructor = Persistent(ctor);
  constructor.SuppressDestruct();
  target.Set("Reader", ctor);
}
Reader::Reader(const CallbackInfo& info)
  : ObjectWrap(info)
{
  string input = info[0].As<String>();
  reader = createReader(readFile(input), readerOptions);
  for (unsigned int i = 0; i < reader->getType().getSubtypeCount(); i++) {
    TypeKind columnType = reader->getType().getSubtype(i)->getKind();
    string columnTitle = reader->getType().getFieldName(i);
    fileMeta.emplace_back(NorcColumnMetadata{ columnTitle, i, columnType });
  }
}
Reader::~Reader()
{
  cout << "Destructing Reader" << endl;
}
class ReadWorker : public AsyncWorker
{
public:
  ReadWorker(Function& cb, Napi::Value self, list<uint64_t> includes)
    : AsyncWorker(cb, "read_worker", self.As<Object>())
    , reader(Reader::Unwrap(self.As<Object>()))
    , includes(std::move(includes))
  {
    HandleScope scope(Env());
  }
  vector<string> data;
  string full;
  bool asIterator = false;

protected:
  void Execute() override
  {
    try {
      RowReaderOptions options;
      if (!includes.empty()) {
        options.include(includes);
      }
      unique_ptr<RowReader> row = reader->reader->createRowReader(options);
      unique_ptr<ColumnVectorBatch> batch = row->createRowBatch(1024);
      string line;
      unique_ptr<ColumnPrinter> printer =
        createColumnPrinter(line, &row->getSelectedType());
      while (row->next(*batch)) {
        printer->reset(*batch);
        for (unsigned int i = 0; i < batch->numElements; i++) {
          printer->printRow(i);
          if (asIterator) {
            data.emplace_back(line);
          }
          full += line + ",";
          line = "";
        }
      }
      cout << full[full.size() - 1] << endl;
      full.erase(full.size() - 1);
      cout << full[full.size() - 1] << endl;
    } catch (std::exception& ex) {
      if (!asIterator) {
        Function emit = reader->Value().Get("emit").As<Function>();
        emit.Call(reader->Value(),
                  { String::New(reader->Env(), "error"),
                    String::New(reader->Env(), ex.what()) });
      } else {
        SetError(ex.what());
      }
    }
  }
  void OnOK() override
  {
    reader->data = Buffer<string>::New(Env(), &full, full.size());
    if (asIterator) {
      Array out = Array::New(Env());
      for (uint32_t i = 0; i < data.size(); i++) {
        auto o = Object::New(Env());
        json j = json::parse(data[i]);
        for (json::iterator it = j.begin(); it != j.end(); ++it) {
          const string& k = it.key();
          if (it.value().is_boolean()) {
            o.Set(k, Boolean::New(Env(), it.value()));
          } else if (it.value().is_number() || it.value().is_number_float() ||
                     it.value().is_number_integer() ||
                     it.value().is_number_unsigned()) {
            o.Set(k, Number::New(Env(), it.value()));
          } else if (it.value().is_string()) {
            string v = it.value();
            o.Set(k, String::New(Env(), v));
          } else if (it.value().is_null()) {
            o.Set(k, Env().Null());
          } else {
            o.Set(
              k,
              Error::New(Env(),
                         "Unable to convert " + k + " from string: " + data[i])
                .Value());
          }
        }
        out.Set(i, o);
      }

      Callback().Call({ Env().Null(),
                        out.Get(Symbol::WellKnown(Env(), "iterator"))
                          .As<Function>()
                          .Call(out, {}) });
    } else {
      Function emit = reader->Value().Get("emit").As<Function>();
      auto chunkCount = ceil(static_cast<double>(reader->data.Data()->size()) /
                             reader->chunkSize);
      unsigned long offset = 0;
      bool run = true;
      while(run){
        unsigned long chunkEnd;
        if(offset + reader->chunkSize > reader->data.Data()->size()) {
          chunkEnd =  reader->data.Data()->size() - offset;
          if(chunkEnd == 0)
            break;
          run = false;
        } else {
          chunkEnd =  reader->chunkSize + offset;
        }

        auto endIndex = reader->data.Data()->find('}', chunkEnd);
        string sub = reader->data.Data()->substr(offset, ++endIndex - offset);
        if (sub.empty())
          break;
        if (sub[0] == ',')
          sub.erase(0, 1);
        if (sub[sub.size() - 1] == ',')
          sub.erase(sub.size() - 1);

        offset = endIndex;
        emit.Call(
          reader->Value(),
          { String::New(Env(), "data"), String::New(Env(), '[' + sub + ']') });
      }

      emit.Call(reader->Value(), { String::New(Env(), "end") });
    }
  }

private:
  Reader* reader;
  list<uint64_t> includes;
};
void
Reader::Read(const CallbackInfo& info)
{
  // TODO: This is not right, remove instance Napi::Buffer, replace with std vector, or just remove
  if (data && !data.Data()->empty()) {
    cout << "Clearing previous read results" << endl;
  }
  vector<int> opts =
    AssertCallbackInfo(info,
                       { { 0, { option(napi_function), option(napi_object) } },
                         { 1, { nullopt, option(napi_function) } } });
  Function cb;
  Array cols;
  list<uint64_t> indices;
  bool asIter = false;
  if (opts[0] == 0) {
    cb = info[0].As<Function>();
  } else if (opts[0] == 1) {
    Object options = info[0].As<Object>();
    if (options.Has("columns")) {
      cols = options.Get("columns").As<Array>();
    }
    if (options.Has("resultType")) {
      string rt = options.Get("resultType").As<String>();
      if (rt == "iterator") {
        asIter = true;
      }
    }
    if (!cols.IsEmpty()) {
      for (unsigned int i = 0; i < cols.Length(); i++) {
        if (cols.Get(static_cast<uint32_t>(i)).IsString()) {
          string title = cols.Get(static_cast<uint32_t>(i)).As<String>();
          bool match = false;
          for (unsigned int j = 0; j < fileMeta.size(); j++) {
            if (fileMeta[j].title == title) {
              indices.emplace_back(j);
              match = true;
              break;
            }
          }
          if (!match) {
            Error::New(info.Env(), title + " not a valid column header")
              .ThrowAsJavaScriptException();
            return;
          }
        }
      }
    }
  }
  if (opts[1] == 1) {
    cb = info[1].As<Function>();
  }
  ReadWorker* worker = new ReadWorker(cb, info.This(), indices);
  if (asIter) {
    worker->asIterator = true;
  }
  worker->Queue();
}

Napi::Value
Reader::GetCompression(const CallbackInfo& info)
{
  return String::New(info.Env(),
                     orc::compressionKindToString(reader->getCompression()));
}
Napi::Value
Reader::GetWriterVersion(const CallbackInfo& info)
{
  return String::New(info.Env(),
                     orc::writerVersionToString(reader->getWriterVersion()));
}
Napi::Value
Reader::GetType(const CallbackInfo& info)
{
  return String::New(info.Env(), reader->getType().toString());
}
Napi::Value
Reader::GetColumnStatistics(const CallbackInfo& info)
{
  string columnTitle = info[0].As<String>();
  for (auto i : fileMeta) {
    if (i.title == columnTitle) {
      return String::New(info.Env(),
                         reader->getColumnStatistics(i.index)->toString());
    }
  }
  Error::New(info.Env(), "Column: " + columnTitle + " not found")
    .ThrowAsJavaScriptException();
  return {};
}
Napi::Value
Reader::GetFormatVersion(const CallbackInfo& info)
{
  return String::New(info.Env(), reader->getFormatVersion().toString());
}
}
