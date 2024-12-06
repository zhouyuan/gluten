
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <iostream>
#include "udf/Udf.h"
#include "udf/examples/UdfCommon.h"
#include <string>
#include <vector>
#include <sstream>
#include <stdexcept>
        
using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

static const char* kString="string";
static const char* kDouble="double";

double cosineSimilarity(const std::vector<float>& vectorA, const std::vector<float>& vectorB) {
double intersectionScore = 0.0;
double aScore = 0.0;
double bScore = 0.0;
for (size_t i = 0; i < vectorA.size(); ++i) {
intersectionScore += vectorA[i] * vectorB[i];
aScore += std::pow(vectorA[i], 2);
bScore += std::pow(vectorB[i], 2);
}
if (aScore == 0 || bScore == 0) {
return 0.0;
} else {
return intersectionScore / (std::sqrt(aScore) * std::sqrt(bScore));
}
}
std::vector<float> parseVectorFromString(const std::string& vectorString) {
std::vector<float> vector;
std::stringstream ss(vectorString);
std::string item;
while (std::getline(ss, item, ',')) {
vector.push_back(std::stof(item));
}
return vector;
}
    
template <typename T>
struct ListVectorSimilarityUDF {
  VELOX_DEFINE_FUNCTION_TYPES(T);


void call(out_type<double>& out, const arg_type<Varchar>& arg1, const arg_type<Varchar>& arg2, const arg_type<Varchar>& arg3) {
	static constexpr const char* COSINE_SIMILARITY = "cosine";
auto cppArg3 = std::string(arg3);
auto cppArg2 = std::string(arg2);
auto cppArg1 = std::string(arg1);
if (cppArg1.empty() || cppArg2.empty() || cppArg3.empty()) {
throw std::invalid_argument("invalid arguments");
}
std::vector<float> vectorA = parseVectorFromString(cppArg1);
std::vector<float> vectorB = parseVectorFromString(cppArg2);
if (vectorA.empty() || vectorB.empty()) {
{out = 0.0;
return;} // In C++, we typically don't return null for non-pointer types.
}
if (cppArg3 == COSINE_SIMILARITY) {
{out = cosineSimilarity(vectorA, vectorB);
return;}
} else {
{out = 0.0;
return;} // In C++, we typically don't return null for non-pointer types.
}
}


};
        
class ListVectorSimilarityUDFRegisterer final : public gluten::UdfRegisterer {
 public:
  int getNumUdf() override {
    return 1;
  }

  void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) override {
    udfEntries[index++] = {name_.c_str(), kDouble, 3, myArg_};
  }

  void registerSignatures() override {
    facebook::velox::registerFunction<ListVectorSimilarityUDF, double, Varchar, Varchar, Varchar>({name_});
  }

 private:
  const std::string name_ = "com.pinterest.hadoop.hive.ListVectorSimilarityUDF";
  const char* myArg_[3] = {kString ,kString ,kString};
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

  registerers.push_back(std::make_shared<ListVectorSimilarityUDFRegisterer>());

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