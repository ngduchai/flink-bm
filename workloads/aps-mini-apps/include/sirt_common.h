#ifndef COMMON_H
#define COMMON_H

#include <unordered_map>
#include <string>
#include <stdexcept>

// static const int64_t require_int(
//   const std::unordered_map<std::string, int64_t>& m,
//   const char* k);

static const int64_t require_int(
  const std::unordered_map<std::string, int64_t>& m,
      const char* k) {
  auto it = m.find(k);
  if (it == m.end())
      throw std::runtime_error(std::string("load int config/metadata: missing key '") + k + "'");
  return it->second;
}

// static const std::string& require_str(
//   const std::unordered_map<std::string, std::string>& m,
//   const char* k);

static const std::string& require_str(
  const std::unordered_map<std::string, std::string>& m,
      const char* k) {
  auto it = m.find(k);
  if (it == m.end())
      throw std::runtime_error(std::string("load str config/metadata: missing key '") + k + "'");
  return it->second;
}

#endif // COMMON_H