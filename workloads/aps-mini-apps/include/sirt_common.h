#ifndef COMMON_H
#define COMMON_H

#include <unordered_map>
#include <string>
#include <stdexcept>
#include <cstring>   // for memcpy or reinterpret_cast
#include <string_view>
#include <cstdint>
#include <vector>

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

inline uint32_t fnv1a32_bytes(const unsigned char* data, size_t nbytes) {
  uint32_t h = 0x811C9DC5u;
  for (size_t i = 0; i < nbytes; ++i) {
      h ^= data[i];
      h *= 0x01000193u;
  }
  return h;
}

// Overload for float pointer
inline uint32_t fnv1a32(const float* data, size_t count) {
  const unsigned char* bytes = reinterpret_cast<const unsigned char*>(data);
  return fnv1a32_bytes(bytes, count * sizeof(float));
}

// Overload for std::vector<float>
inline uint32_t fnv1a32(const std::vector<float>& v) {
  return fnv1a32(v.data(), v.size());
}

// Overload for std::string_view (for text data)
inline uint32_t fnv1a32(std::string_view s) {
  return fnv1a32_bytes(reinterpret_cast<const unsigned char*>(s.data()), s.size());
}

#endif // COMMON_H