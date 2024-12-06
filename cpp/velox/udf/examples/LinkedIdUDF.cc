
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <iostream>
#include "udf/Udf.h"
#include "udf/examples/UdfCommon.h"
#include <string>
#include <stdexcept>
#include <memory>
#include <openssl/md5.h>
        
using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

static const char* kString="string";

std::string GetHashKey(const std::string& str) {
if (str.empty()) {
throw std::invalid_argument("Input string cannot be null");
}
unsigned char digest[MD5_DIGEST_LENGTH];
MD5(reinterpret_cast<const unsigned char*>(str.c_str()), str.size(), digest);
std::string key;
key.reserve(MD5_DIGEST_LENGTH * 2);
for (unsigned char i : digest) {
char buf[3];
snprintf(buf, sizeof(buf), "%02x", i);
key.append(buf);
}
return key;
}
    
template <typename T>
struct LinkIdUDF {
  VELOX_DEFINE_FUNCTION_TYPES(T);


void call(out_type<Varchar>& out, const arg_type<Varchar>& arg1) {
	
auto cppArg1 = std::string(arg1);
if (cppArg1.empty()) {
{out = "";
return;}
}
{out = GetHashKey(cppArg1);
return;}
}


};
        
class LinkIdUDFRegisterer final : public gluten::UdfRegisterer {
 public:
  int getNumUdf() override {
    return 1;
  }

  void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) override {
    udfEntries[index++] = {name_.c_str(), kString, 1, myArg_};
  }

  void registerSignatures() override {
    facebook::velox::registerFunction<LinkIdUDF, Varchar, Varchar>({name_});
  }

 private:
  const std::string name_ = "com.pinterest.hadoop.hive.LinkIdUDF";
  const char* myArg_[1] = {kString};
};
            
std::vector<std::shared_ptr<gluten::UdfRegisterer>>& globalRegisters() {
  static std::vector<std::shared_ptr<gluten::UdfRegisterer>> registerers;
  return registerers;
}

void setupRegisterers() {
  static bool inited = false;
  if (inited) {
    return;
  }
  auto& registerers = globalRegisters();

  registerers.push_back(std::make_shared<LinkIdUDFRegisterer>());

  inited = true;
}
} // namespace

DEFINE_GET_NUM_UDF {
  setupRegisterers();

  int numUdf = 0;
  for (const auto& registerer : globalRegisters()) {
    numUdf += registerer->getNumUdf();
  }
  return numUdf;
}

DEFINE_GET_UDF_ENTRIES {
  setupRegisterers();

  int index = 0;
  for (const auto& registerer : globalRegisters()) {
    registerer->populateUdfEntries(index, udfEntries);
  }
}

DEFINE_REGISTER_UDF {
  setupRegisterers();

  for (const auto& registerer : globalRegisters()) {
    registerer->registerSignatures();
  }
}
