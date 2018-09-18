//
// Created by developer on 9/13/18.
//

#include <napi.h>
#include "Writer.h"
#include "Reader.h"

using namespace Napi;

Napi::Object
Init(Napi::Env env, Napi::Object target) {
    norc::Writer::Initialize(env, target);
    norc::Reader::Initialize(env, target);
    return target;
}

NODE_API_MODULE(norc, Init)
