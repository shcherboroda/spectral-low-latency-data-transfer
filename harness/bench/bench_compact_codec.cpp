// Reproducible microbenchmark for compact codec work; it does no I/O or syscalls.
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "compact_wire.h"

namespace {

volatile uint64_t sink = 0;

void set_string(char* destination, size_t cap, const char* source) {
  std::memset(destination, 0, cap);
  std::strncpy(destination, source, cap - 1);
}

void fill_common(msg::Header* header, uint64_t seq, msg::Type type, uint32_t size) {
  *header = {seq, 1000000000ull + seq, static_cast<uint16_t>(type), 1, size};
}

msg::Trade make_trade() {
  msg::Trade m{};
  fill_common(&m.header, 101, msg::Type::Trade, sizeof(m));
  set_string(m.symbol, sizeof(m.symbol), "BTCUSDT");
  set_string(m.venue, sizeof(m.venue), "BINANCE");
  set_string(m.base_currency, sizeof(m.base_currency), "BTC");
  set_string(m.quote_currency, sizeof(m.quote_currency), "USDT");
  m.trade_id = 100101; m.buyer_order_id = 500202; m.seller_order_id = 500203;
  m.exchange_ts_ns = m.match_engine_ts_ns = 2000101;
  m.price = 65050.5; m.quantity = 1.111; m.notional = m.price * m.quantity;
  m.price_ticks = 6505050; m.quantity_lots = 1110;
  m.tick_direction = 2; m.aggressor_side = 1; m.flags = 3;
  return m;
}

msg::Bbo make_bbo() {
  msg::Bbo m{};
  fill_common(&m.header, 102, msg::Type::Bbo, sizeof(m));
  set_string(m.symbol, sizeof(m.symbol), "BTCUSDT"); set_string(m.venue, sizeof(m.venue), "BINANCE");
  m.update_id = compact_wire::kUpdateOffset + m.header.seq_id;
  m.exchange_ts_ns = m.match_engine_ts_ns = 2000102;
  m.bid_price = 65050.0; m.bid_size = 1.1; m.ask_price = 65051.0; m.ask_size = 2.2;
  m.bid_price_ticks = 6505000; m.ask_price_ticks = 6505100; m.bid_size_lots = 1100; m.ask_size_lots = 2200;
  m.bid_order_count = 4; m.ask_order_count = 5; m.flags = 3;
  return m;
}

msg::OrderBook make_book() {
  msg::OrderBook m{};
  fill_common(&m.header, 103, msg::Type::OrderBook, sizeof(m));
  set_string(m.symbol, sizeof(m.symbol), "BTCUSDT"); set_string(m.venue, sizeof(m.venue), "BINANCE");
  m.update_id = compact_wire::kUpdateOffset + m.header.seq_id;
  m.prev_update_id = m.update_id - 1; m.exchange_ts_ns = m.match_engine_ts_ns = 2000103;
  for (uint32_t i = 0; i < msg::kBookDepth; ++i) {
    m.bids[i] = {65049.0 - i, 1.0 + i * 0.1, 6504900 - static_cast<int64_t>(i * 100), 1000 + static_cast<int64_t>(i * 100), 2 + i, 0};
    m.asks[i] = {65051.0 + i, 1.5 + i * 0.1, 6505100 + static_cast<int64_t>(i * 100), 1500 + static_cast<int64_t>(i * 100), 3 + i, 0};
  }
  m.checksum = 123456; m.is_snapshot = 1; m.flags = 3;
  return m;
}

template <typename T>
void run(const char* name, const T& source, uint64_t iterations) {
  alignas(64) uint8_t encoded[msg::kMaxFrame]{};
  alignas(64) uint8_t expanded[msg::kMaxFrame]{};
  uint32_t encoded_len = 0, expanded_len = 0;
  if (!compact_wire::encode(reinterpret_cast<const uint8_t*>(&source), sizeof(source), encoded, &encoded_len)) std::abort();

  const auto encode_start = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < iterations; ++i) {
    uint32_t len = 0;
    if (!compact_wire::encode(reinterpret_cast<const uint8_t*>(&source), sizeof(source), encoded, &len)) std::abort();
    sink += len;
  }
  const auto encode_end = std::chrono::steady_clock::now();

  const auto decode_start = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < iterations; ++i) {
    if (!compact_wire::decode(encoded, encoded_len, expanded, &expanded_len)) std::abort();
    sink += expanded_len;
  }
  const auto decode_end = std::chrono::steady_clock::now();
  const auto encode_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(encode_end - encode_start).count();
  const auto decode_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(decode_end - decode_start).count();
  std::printf("%s bytes=%u encode_ns_per_op=%.1f decode_ns_per_op=%.1f\n", name, encoded_len,
              static_cast<double>(encode_ns) / iterations, static_cast<double>(decode_ns) / iterations);
}

}  // namespace

int main(int argc, char** argv) {
  const uint64_t iterations = argc == 2 ? std::strtoull(argv[1], nullptr, 10) : 5000000ull;
  if (iterations == 0) return 2;
  run("trade", make_trade(), iterations);
  run("bbo", make_bbo(), iterations);
  run("book", make_book(), iterations);
  std::printf("sink=%llu\n", static_cast<unsigned long long>(sink));
  return 0;
}
