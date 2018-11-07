//
// Created by developer on 11/6/18.
//

#ifndef NORC_INTERNAL_H
#define NORC_INTERNAL_H

#include <napi.h>
#include <orc/OrcFile.hh>

namespace norc {

void
LineToJSON(Napi::Env, std::string&, Napi::Object&);
void
AddNumberType(Napi::Env, orc::ColumnVectorBatch*, uint64_t, Napi::Value);
void
AddStringType(Napi::Env, orc::ColumnVectorBatch*, orc::DataBuffer<char>*, uint64_t batchOffset, uint64_t* bufferOffset, Napi::Value);
void
AddBoolType(Napi::Env, orc::ColumnVectorBatch*, uint64_t, Napi::Value);
void
AddFloatType(Napi::Env, orc::ColumnVectorBatch*, uint64_t, Napi::Value);
void
AddTimeType(Napi::Env, orc::ColumnVectorBatch*, uint64_t, Napi::Value);
void
AddDecimalType(Napi::Env, orc::ColumnVectorBatch*, const orc::Type*, uint64_t batchOffset, int index, Napi::Value);
void
AddDateType(Napi::Env, orc::ColumnVectorBatch*, uint64_t batchOffset, Napi::Value);
}

#endif // NORC_INTERNAL_H
