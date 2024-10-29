#include "reassembler.hh"
#include <algorithm>

using namespace std;

void Reassembler::insert(uint64_t first_index, std::string data, bool is_last_substring)
{
  Writer& writer = output_.writer();
  const uint64_t max_index = expecting_index_ + writer.available_capacity();
  if (writer.is_closed() || writer.available_capacity() == 0 || first_index >= max_index)
    return; 
  else if (first_index + data.size() >= max_index) {
    is_last_substring = false;                     
    data.resize(max_index - first_index); 
  }
  // 如果不是下一段的数据，先缓存起来
  if (first_index > expecting_index_)
    bytes2cache(first_index, std::move(data), is_last_substring);
  // 如果是下一段的数据，直接写入writer
  else
    bytes2writers(first_index, std::move(data), is_last_substring);
  flush_buffer();
}

uint64_t Reassembler::bytes_pending() const
{
  return num_bytes_pending_;
}

void Reassembler::bytes2writers(uint64_t first_index, std::string data, bool is_last_substring)
{
  // 如果first_index小于expecting_index_，说明有数据重复了，需要去掉重复的部分
  if (first_index < expecting_index_) 
    data.erase(0, expecting_index_ - first_index);
  expecting_index_ += data.size();
  output_.writer().push(std::move(data));
  // 如果这是最后一段数据，关闭输出流
  if (is_last_substring) {
    output_.writer().close(); 
    cache.clear();
    num_bytes_pending_ = 0;
  }
}

void Reassembler::bytes2cache(uint64_t first_index, std::string data, bool is_last_substring)
{
  auto end = cache.end();
  
  auto left = std::lower_bound(cache.begin(), end, first_index,
                             [](auto&& e, uint64_t idx) -> bool {
                                 return idx > (get<0>(e) + get<1>(e).size());
                             });
  auto right = std::upper_bound(left, end, first_index + data.size(),
                              [](uint64_t nxt_idx, auto&& e) -> bool {
                                  return nxt_idx < get<0>(e);
                              });
  
  if (const uint64_t next_index = first_index + data.size(); left != end) {
    auto& [l_point, dat, _] = *left;
    if (const uint64_t r_point = l_point + dat.size(); first_index >= l_point && next_index <= r_point)
      return; 
    else if (next_index < l_point) {
      right = left;                                                      
    } else if ( !(first_index <= l_point && r_point <= next_index) ) { 
      if (first_index >= l_point) {                                    
        data.insert(0, std::string_view(dat.c_str(), dat.size() - (r_point - first_index)));
      } else {
        data.resize(data.size() - (next_index - l_point));
        data.append(dat); 
      }
      first_index = std::min(first_index, l_point);
    }
  }

  if (const uint64_t next_index = first_index + data.size(); right != left && !cache.empty()) {
    
    auto& [l_point, dat, _] = *std::prev(right);
    if (const uint64_t r_point = l_point + dat.size(); r_point > next_index) {
      data.resize(data.size() - (next_index - l_point));
      data.append(dat);
    }
  }

  for (; left != right; left = cache.erase(left)) {
    num_bytes_pending_ -= get<1>(*left).size();
    is_last_substring |= get<2>(*left);
  }
  num_bytes_pending_ += data.size();
  cache.insert(left, {first_index, std::move(data), is_last_substring});
}

void Reassembler::flush_buffer()
{
  while (!cache.empty()) {
    auto& [idx, dat, last] = cache.front();
    if (idx > expecting_index_)
      break;                          // 乱序的，不做任何动作
    num_bytes_pending_ -= dat.size(); // 数据已经被填补上了，立即推入写端
    bytes2writers(idx, std::move(dat), last);
    if (!cache.empty())
      cache.pop_front();
  }
}

