// Standalone assertion-based tests for the metrics accumulator and the shm ring.
// No test framework -- just asserts, so this stays dependency-free and builds
// with a single g++ invocation.
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <vector>

#include "metrics.h"
#include "compact_wire.h"
#include "fec_wire.h"
#include "fec_xor.h"
#include "fec_rs.h"
#include "packet_fec_wire.h"
#include "shm_ring.h"
#include "udp_deduper.h"
#include "udp_resequencer.h"
#include "udp_wire.h"
#include "udp_batch_wire.h"
#include "stage_metrics.h"

static void test_metrics_basic() {
  metrics::Accumulator acc;
  // seq 1..100 all delivered, latency == seq nanoseconds.
  for (uint64_t i = 1; i <= 100; ++i) acc.record(i, i);
  metrics::Report r = acc.report();

  assert(r.received == 100);
  assert(r.expected == 100);
  assert(r.dropped == 0);
  assert(r.drop_rate == 0.0);
  assert(r.lat_min == 1);
  assert(r.lat_max == 100);
  // Nearest-rank: p50 of 1..100 -> rank ceil(0.5*100)=50 -> value 50.
  assert(r.p50 == 50);
  assert(r.p99 == 99);
  assert(r.lat_mean > 50.0 && r.lat_mean < 51.0);
  printf("test_metrics_basic OK\n");
}

static void test_metrics_drops() {
  metrics::Accumulator acc;
  // Deliver only even sequence ids 2,4,...,100 -> 50 received, 100 expected.
  for (uint64_t i = 2; i <= 100; i += 2) acc.record(i, 10);
  metrics::Report r = acc.report();

  assert(r.received == 50);
  assert(r.expected == 99);  // last(100) - first(2) + 1
  assert(r.dropped == 49);
  assert(r.drop_rate > 0.49 && r.drop_rate < 0.50);
  printf("test_metrics_drops OK\n");
}

static void test_stage_metrics_tail_report() {
  stage_metrics::Samples samples;
  for (uint64_t i = 1; i <= 100; ++i) samples.record(i);
  FILE* file = std::tmpfile();
  assert(file != nullptr);
  samples.print(file, "stage");
  std::rewind(file);
  char text[512]{};
  const size_t read = std::fread(text, 1, sizeof(text) - 1, file);
  std::fclose(file);
  assert(read != 0);
  assert(std::strstr(text, "stage_p999_ns=100") != nullptr);
  assert(std::strstr(text, "stage_p9999_ns=100") != nullptr);
  printf("test_stage_metrics_tail_report OK\n");
}

static void test_ring_roundtrip() {
  const uint32_t slots = 8;
  std::vector<uint8_t> mem(shm::region_size(slots));
  shm::Ring prod;
  prod.attach(mem.data(), slots, /*init=*/true);
  shm::Ring cons;
  cons.attach(mem.data(), slots, /*init=*/false);

  // Publish 5 frames (fits in the ring, no lapping).
  for (uint32_t i = 0; i < 5; ++i) {
    uint8_t frame[16];
    std::memset(frame, static_cast<int>(i), sizeof(frame));
    prod.publish(frame, sizeof(frame));
  }

  uint64_t read_index = 0;
  for (uint32_t i = 0; i < 5; ++i) {
    uint8_t out[64];
    uint32_t len = 0;
    uint64_t resume = 0;
    auto st = cons.read(read_index, out, &len, &resume);
    assert(st == shm::Ring::FrameStatus::kOk);
    assert(len == 16);
    assert(out[0] == static_cast<uint8_t>(i));
    ++read_index;
  }
  // Next read is empty (nothing published yet).
  uint8_t out[64];
  uint32_t len = 0;
  uint64_t resume = 0;
  assert(cons.read(read_index, out, &len, &resume) ==
         shm::Ring::FrameStatus::kEmpty);
  printf("test_ring_roundtrip OK\n");
}

static void test_ring_lapping() {
  const uint32_t slots = 4;
  std::vector<uint8_t> mem(shm::region_size(slots));
  shm::Ring prod;
  prod.attach(mem.data(), slots, /*init=*/true);
  shm::Ring cons;
  cons.attach(mem.data(), slots, /*init=*/false);

  // Publish 10 frames into a 4-slot ring -> reader sitting at index 0 is lapped.
  for (uint32_t i = 0; i < 10; ++i) {
    uint8_t frame[8];
    std::memset(frame, static_cast<int>(i), sizeof(frame));
    prod.publish(frame, sizeof(frame));
  }

  uint8_t out[64];
  uint32_t len = 0;
  uint64_t resume = 0;
  auto st = cons.read(0, out, &len, &resume);
  assert(st == shm::Ring::FrameStatus::kLapped);
  // Producer wrote 10, ring holds 4 -> safe resume position is 10 - 4 = 6.
  assert(resume == 6);

  // Reading from the resume point yields the frame published at index 6.
  st = cons.read(resume, out, &len, &resume);
  assert(st == shm::Ring::FrameStatus::kOk);
  assert(out[0] == 6);
  printf("test_ring_lapping OK\n");
}

static void test_udp_wire_roundtrip_and_rejection() {
  alignas(64) uint8_t packet[udp_wire::kMaxDatagram]{};
  msg::Trade trade{};
  trade.header.seq_id = 42;
  trade.header.send_ts_ns = 123;
  trade.header.type = static_cast<uint16_t>(msg::Type::Trade);
  trade.header.version = 1;
  trade.header.body_len = sizeof(trade);
  udp_wire::encode(packet, 99, trade.header.seq_id, sizeof(trade));
  std::memcpy(packet + udp_wire::kHeaderSize, &trade, sizeof(trade));

  udp_wire::DecodedHeader decoded{};
  assert(udp_wire::decode(packet, udp_wire::kHeaderSize + sizeof(trade), &decoded));
  assert(decoded.session_id == 99);
  assert(decoded.seq_id == 42);
  assert(decoded.frame_len == sizeof(trade));
  assert(decoded.format == udp_wire::Format::kFull);
  assert(udp_wire::valid_frame(packet + udp_wire::kHeaderSize, decoded.frame_len, decoded.seq_id));
  assert(!udp_wire::decode(packet, udp_wire::kHeaderSize + sizeof(trade) - 1, &decoded));
  assert(!udp_wire::valid_frame(packet + udp_wire::kHeaderSize, decoded.frame_len, 43));

  alignas(64) uint8_t corrupt[udp_wire::kMaxDatagram]{};
  std::memcpy(corrupt, packet, udp_wire::kHeaderSize + sizeof(trade));
  corrupt[0] ^= 1;
  assert(!udp_wire::decode(corrupt, udp_wire::kHeaderSize + sizeof(trade), &decoded));
  std::memcpy(corrupt, packet, udp_wire::kHeaderSize + sizeof(trade));
  corrupt[4] = 0;
  corrupt[5] = 2;
  assert(!udp_wire::decode(corrupt, udp_wire::kHeaderSize + sizeof(trade), &decoded));
  trade.header.body_len = sizeof(trade) - 1;
  assert(!udp_wire::valid_frame(&trade, sizeof(trade), trade.header.seq_id));
  printf("test_udp_wire_roundtrip_and_rejection OK\n");
}

static void set_constant(char* destination, size_t cap, const char* source) {
  std::memset(destination, 0, cap);
  std::strncpy(destination, source, cap - 1);
}

template <typename T>
static void assert_compact_roundtrip(const T& source) {
  alignas(64) uint8_t encoded[msg::kMaxFrame]{};
  alignas(64) uint8_t decoded[msg::kMaxFrame]{};
  uint32_t encoded_len = 0, decoded_len = 0;
  assert(compact_wire::encode(reinterpret_cast<const uint8_t*>(&source), sizeof(source),
                              encoded, &encoded_len));
  assert(encoded_len == compact_wire::encoded_size(source.header));
  assert(compact_wire::decode(encoded, encoded_len, decoded, &decoded_len));
  assert(decoded_len == sizeof(source));
  assert(std::memcmp(&source, decoded, sizeof(source)) == 0);
}

static void test_compact_wire_roundtrip() {
  msg::Trade trade{};
  trade.header = {7, 1007, static_cast<uint16_t>(msg::Type::Trade), 1, sizeof(trade)};
  set_constant(trade.symbol, sizeof(trade.symbol), "BTCUSDT");
  set_constant(trade.venue, sizeof(trade.venue), "BINANCE");
  set_constant(trade.base_currency, sizeof(trade.base_currency), "BTC");
  set_constant(trade.quote_currency, sizeof(trade.quote_currency), "USDT");
  trade.trade_id = 100007; trade.buyer_order_id = 500014; trade.seller_order_id = 500015;
  trade.exchange_ts_ns = trade.match_engine_ts_ns = 2007;
  trade.price = 65003.0; trade.quantity = 1.25; trade.notional = trade.price * trade.quantity;
  trade.price_ticks = 6500300; trade.quantity_lots = 1250; trade.tick_direction = 3;
  trade.aggressor_side = 1; trade.is_block_trade = 1; trade.is_rpi = 0; trade.is_liquidation = 1; trade.flags = 9;
  assert_compact_roundtrip(trade);
  trade.symbol[0] = 'X';
  assert_compact_roundtrip(trade);
  trade.symbol[0] = 'B';
  trade.notional += 1.0;
  alignas(64) uint8_t rejected[msg::kMaxFrame]{};
  uint32_t rejected_len = 0;
  assert(!compact_wire::encode(reinterpret_cast<const uint8_t*>(&trade), sizeof(trade), rejected, &rejected_len));

  msg::Bbo bbo{};
  bbo.header = {8, 1008, static_cast<uint16_t>(msg::Type::Bbo), 1, sizeof(bbo)};
  set_constant(bbo.symbol, sizeof(bbo.symbol), "BTCUSDT"); set_constant(bbo.venue, sizeof(bbo.venue), "BINANCE");
  bbo.update_id = 900008; bbo.exchange_ts_ns = bbo.match_engine_ts_ns = 2008;
  bbo.bid_price = 65000.5; bbo.bid_size = 1.1; bbo.ask_price = 65001.5; bbo.ask_size = 2.2;
  bbo.bid_price_ticks = 6500050; bbo.ask_price_ticks = 6500150; bbo.bid_size_lots = 1100; bbo.ask_size_lots = 2200;
  bbo.bid_order_count = 4; bbo.ask_order_count = 7; bbo.flags = 5;
  assert_compact_roundtrip(bbo);

  msg::OrderBook book{};
  book.header = {9, 1009, static_cast<uint16_t>(msg::Type::OrderBook), 1, sizeof(book)};
  set_constant(book.symbol, sizeof(book.symbol), "BTCUSDT"); set_constant(book.venue, sizeof(book.venue), "BINANCE");
  book.update_id = 900009; book.prev_update_id = 900008; book.exchange_ts_ns = book.match_engine_ts_ns = 2009;
  for (uint32_t i = 0; i < msg::kBookDepth; ++i) {
    book.bids[i] = {65000.0 - i, 1.0 + static_cast<double>(i) * 0.01, 6500000 - static_cast<int64_t>(i * 100), 1000 + static_cast<int64_t>(i * 10), 2 + i, 0};
    book.asks[i] = {65001.0 + i, 2.0 + static_cast<double>(i) * 0.01, 6500100 + static_cast<int64_t>(i * 100), 2000 + static_cast<int64_t>(i * 10), 3 + i, 0};
  }
  book.checksum = 123456; book.is_snapshot = 1; book.flags = 6;
  assert_compact_roundtrip(book);
  book.prev_update_id = 1;
  assert(!compact_wire::encode(reinterpret_cast<const uint8_t*>(&book), sizeof(book), rejected, &rejected_len));
  printf("test_compact_wire_roundtrip OK\n");
}

static void test_resequencer_order_and_deadline() {
  udp_resequencer::Window window(8);
  uint8_t one[] = {1}, two[] = {2}, three[] = {3}, ten[] = {10};
  std::vector<uint64_t> released;
  auto publish = [&](const uint8_t* frame, uint32_t) { released.push_back(frame[0]); };
  assert(window.insert(1, one, sizeof(one), 100) == udp_resequencer::InsertResult::kBuffered);
  window.release_ready(100, 50, publish);
  assert((released == std::vector<uint64_t>{1}));
  assert(window.insert(3, three, sizeof(three), 110) == udp_resequencer::InsertResult::kBuffered);
  window.release_ready(140, 50, publish);
  assert((released == std::vector<uint64_t>{1}));
  assert(window.insert(2, two, sizeof(two), 145) == udp_resequencer::InsertResult::kBuffered);
  window.release_ready(145, 50, publish);
  assert((released == std::vector<uint64_t>{1, 2, 3}));
  assert(window.insert(5, ten, sizeof(ten), 200) == udp_resequencer::InsertResult::kBuffered);
  assert(window.insert(7, ten, sizeof(ten), 201) == udp_resequencer::InsertResult::kBuffered);
  window.release_ready(250, 50, publish);
  assert((released == std::vector<uint64_t>{1, 2, 3, 10}));
  // The second gap (seq 6) starts only after seq 4 was declared lost.
  window.release_ready(299, 50, publish);
  assert(released.size() == 4);
  window.release_ready(300, 50, publish);
  assert(window.counters().declared_drops == 2);
  assert((released == std::vector<uint64_t>{1, 2, 3, 10, 10}));
  assert(window.insert(3, three, sizeof(three), 260) == udp_resequencer::InsertResult::kDuplicateOrLate);

  udp_resequencer::Window immediate(8);
  std::vector<uint64_t> immediate_released;
  auto immediate_publish = [&](const uint8_t* frame, uint32_t) {
    immediate_released.push_back(frame[0]);
  };
  assert(immediate.insert(1, one, sizeof(one), 300) == udp_resequencer::InsertResult::kBuffered);
  immediate.release_ready(300, 0, immediate_publish);
  assert((immediate_released == std::vector<uint64_t>{1}));
  assert(immediate.next_expected() == 2);
  printf("test_resequencer_order_and_deadline OK\n");
}

static void test_fec_xor_single_loss_recovery() {
  alignas(64) uint8_t packet[fec_wire::kMaxDatagram]{};
  fec_wire::encode(packet, fec_wire::Kind::kData, 77, 9, 2, 8, sizeof(msg::Trade));
  fec_wire::Header decoded{};
  assert(fec_wire::decode(packet, fec_wire::kHeaderSize + sizeof(msg::Trade), &decoded));
  assert(decoded.kind == fec_wire::Kind::kData);
  assert(decoded.session_id == 77 && decoded.group_id == 9);
  assert(decoded.shard_index == 2 && decoded.shard_count == 8);
  size_t consumed = 0;
  assert(fec_wire::decode_prefix(packet, fec_wire::kHeaderSize + sizeof(msg::Trade) + 1,
                                 &decoded, &consumed));
  assert(consumed == fec_wire::kHeaderSize + sizeof(msg::Trade));
  assert(!fec_wire::decode(packet, fec_wire::kHeaderSize + sizeof(msg::Trade) + 1,
                           &decoded));

  std::array<msg::Trade, fec_wire::kGroupSize> frames{};
  fec_xor::Encoder encoder;
  uint64_t next_group = 1;
  fec_xor::Encoder::ParityInfo parity{};
  for (uint16_t i = 0; i < fec_wire::kGroupSize; ++i) {
    frames[i].header.seq_id = i + 1;
    frames[i].header.send_ts_ns = 100 + i;
    frames[i].header.type = static_cast<uint16_t>(msg::Type::Trade);
    frames[i].header.version = 1;
    frames[i].header.body_len = sizeof(msg::Trade);
    frames[i].trade_id = 1000 + i;
    fec_xor::Encoder::DataInfo data{};
    const bool completed = encoder.add(reinterpret_cast<const uint8_t*>(&frames[i]),
                                       sizeof(msg::Trade), &next_group, &data, &parity);
    assert(data.group_id == 1 && data.shard_index == i);
    assert(completed == (i + 1 == fec_wire::kGroupSize));
  }
  assert(parity.group_id == 1 && parity.shard_count == fec_wire::kGroupSize);

  fec_xor::Group group;
  const uint16_t missing = 3;
  for (uint16_t i = 0; i < fec_wire::kGroupSize; ++i) {
    if (i == missing) continue;
    const fec_wire::Header header{fec_wire::Kind::kData, 77, 1, i,
                                  fec_wire::kGroupSize, sizeof(msg::Trade)};
    assert(group.add_data(header, reinterpret_cast<const uint8_t*>(&frames[i])));
  }
  const fec_wire::Header parity_header{fec_wire::Kind::kParity, 77, 1,
                                       fec_wire::kGroupSize, fec_wire::kGroupSize,
                                       sizeof(msg::Trade)};
  assert(group.add_parity(parity_header, parity.bytes));
  alignas(64) uint8_t recovered[msg::kMaxFrame]{};
  uint32_t recovered_len = 0;
  assert(group.recover(recovered, &recovered_len));
  assert(recovered_len == sizeof(msg::Trade));
  assert(std::memcmp(recovered, &frames[missing], sizeof(msg::Trade)) == 0);
  printf("test_fec_xor_single_loss_recovery OK\n");
}

static void test_packet_fec_single_datagram_recovery() {
  std::array<std::array<uint8_t, packet_fec_wire::kMaxPayload>, 4> payloads{};
  std::array<uint32_t, 4> lengths{80, 113, 71, 141};
  for (uint32_t i = 0; i < payloads.size(); ++i) {
    udp_batch_wire::encode_packet_fec(payloads[i].data(), 55, 1, lengths[i]);
    for (uint32_t j = static_cast<uint32_t>(udp_batch_wire::kHeaderSize); j < lengths[i]; ++j) {
      payloads[i][j] = static_cast<uint8_t>(i * 31 + j);
    }
  }
  packet_fec_wire::Encoder encoder;
  uint32_t next_group = 1;
  packet_fec_wire::Encoder::ParityInfo parity{};
  std::array<packet_fec_wire::Encoder::DataInfo, 4> data{};
  for (uint32_t i = 0; i < payloads.size(); ++i) {
    const bool complete = encoder.add(payloads[i].data(), lengths[i], &next_group,
                                      &data[i], &parity);
    assert(data[i].group_id == 1 && data[i].shard_index == i);
    assert(complete == (i == 3));
  }
  packet_fec_wire::Group group;
  constexpr uint8_t missing = 2;
  for (uint8_t i = 0; i < 4; ++i) {
    if (i == missing) continue;
    packet_fec_wire::Header header{packet_fec_wire::Kind::kData, i, 0, 55, 1, lengths[i]};
    assert(group.add_data(header, payloads[i].data()));
  }
  packet_fec_wire::Header parity_header{packet_fec_wire::Kind::kParity, 4, 4, 55, 1,
                                        parity.payload_len};
  assert(group.add_parity(parity_header, parity.bytes));
  alignas(64) std::array<uint8_t, packet_fec_wire::kMaxPayload> recovered{};
  uint32_t recovered_len = 0;
  assert(group.recover(recovered.data(), &recovered_len));
  assert(group.complete());
  udp_batch_wire::Header batch{};
  assert(udp_batch_wire::decode(recovered.data(), recovered_len, &batch));
  assert(batch.packet_bytes == lengths[missing]);
  assert(std::memcmp(recovered.data(), payloads[missing].data(), lengths[missing]) == 0);

  packet_fec_wire::Header invalid{};
  alignas(64) std::array<uint8_t, packet_fec_wire::kHeaderSize> zero_payload{};
  packet_fec_wire::encode(zero_payload.data(), packet_fec_wire::Kind::kData,
                          55, 1, 0, 0, 0);
  assert(!packet_fec_wire::decode(zero_payload.data(), zero_payload.size(), &invalid));

  packet_fec_wire::Encoder wrapping_encoder;
  uint32_t next_wrapping_group = std::numeric_limits<uint32_t>::max();
  packet_fec_wire::Encoder::DataInfo wrapping_data{};
  packet_fec_wire::Encoder::ParityInfo wrapping_parity{};
  assert(!wrapping_encoder.add(payloads[0].data(), lengths[0], &next_wrapping_group,
                               &wrapping_data, &wrapping_parity));
  assert(wrapping_data.group_id == std::numeric_limits<uint32_t>::max());
  assert(wrapping_encoder.flush(&wrapping_parity));
  assert(!wrapping_encoder.add(payloads[0].data(), lengths[0], &next_wrapping_group,
                               &wrapping_data, &wrapping_parity));
  assert(wrapping_data.group_id == 1);
  printf("test_packet_fec_single_datagram_recovery OK\n");
}

static void test_fec_rs_field_encoder_recovery_and_flush() {
  for (uint16_t value = 1; value < 256; ++value) assert(fec_rs::mul(static_cast<uint8_t>(value), fec_rs::inv(static_cast<uint8_t>(value))) == 1);
  fec_rs::Encoder encoder;
  uint64_t group_id = 1;
  fec_rs::Encoder::DataInfo data{};
  fec_rs::Encoder::ParityInfo parity{};
  uint8_t frames[8][4]{};
  for (uint16_t i = 0; i < 8; ++i) {
    for (uint8_t& byte : frames[i]) byte = static_cast<uint8_t>(i + 1);
    const bool complete = encoder.add(frames[i], sizeof(frames[i]), &group_id, &data, &parity);
    assert(data.shard_index == i);
    assert(complete == (i == 7));
  }
  uint8_t expected0[4]{}, expected1[4]{};
  for (uint16_t i = 0; i < 8; ++i) {
    fec_rs::add_scaled(expected0, frames[i], 1, sizeof(expected0));
    fec_rs::add_scaled(expected1, frames[i], fec_rs::pow2(i), sizeof(expected1));
  }
  assert(std::memcmp(expected0, parity.bytes[0], sizeof(expected0)) == 0);
  assert(std::memcmp(expected1, parity.bytes[1], sizeof(expected1)) == 0);

  fec_rs::Group complete_group;
  for (uint16_t i = 0; i < fec_wire::kGroupSize; ++i) {
    const fec_wire::Header h{fec_wire::Kind::kData, 7, 10, i, 8,
                             sizeof(frames[i])};
    assert(complete_group.add_data(h, frames[i]));
  }
  const fec_wire::Header complete_parity0{fec_wire::Kind::kParity, 7, 10, 8, 8,
                                          sizeof(frames[0])};
  const fec_wire::Header complete_parity1{fec_wire::Kind::kParity, 7, 10, 9, 8,
                                          sizeof(frames[0])};
  assert(complete_group.add_parity(complete_parity0, parity.bytes[0]));
  assert(!complete_group.complete());
  assert(complete_group.add_parity(complete_parity1, parity.bytes[1]));
  assert(complete_group.complete());

  fec_rs::Group one_loss;
  constexpr uint16_t kOneMissing = 5;
  for (uint16_t i = 0; i < 8; ++i) {
    if (i == kOneMissing) continue;
    const fec_wire::Header h{fec_wire::Kind::kData, 7, 1, i, 8, sizeof(frames[i])};
    assert(one_loss.add_data(h, frames[i]));
  }
  // One loss must also recover when only the weighted parity arrived.
  const fec_wire::Header weighted_parity{fec_wire::Kind::kParity, 7, 1, 9, 8,
                                         sizeof(frames[0])};
  assert(one_loss.add_parity(weighted_parity, parity.bytes[1]));
  std::array<std::array<uint8_t, msg::kMaxFrame>, 2> recovered{};
  uint32_t recovered_len = 0;
  assert(one_loss.recover(&recovered, &recovered_len) == 1);
  assert(recovered_len == sizeof(frames[0]));
  assert(std::memcmp(recovered[0].data(), frames[kOneMissing], sizeof(frames[0])) == 0);

  fec_rs::Group two_loss;
  constexpr uint16_t kFirstMissing = 1;
  constexpr uint16_t kSecondMissing = 6;
  for (uint16_t i = 0; i < 8; ++i) {
    if (i == kFirstMissing || i == kSecondMissing) continue;
    const fec_wire::Header h{fec_wire::Kind::kData, 7, 2, i, 8, sizeof(frames[i])};
    assert(two_loss.add_data(h, frames[i]));
  }
  const fec_wire::Header parity0{fec_wire::Kind::kParity, 7, 2, 8, 8,
                                 sizeof(frames[0])};
  const fec_wire::Header parity1{fec_wire::Kind::kParity, 7, 2, 9, 8,
                                 sizeof(frames[0])};
  assert(two_loss.add_parity(parity0, parity.bytes[0]));
  assert(two_loss.add_parity(parity1, parity.bytes[1]));
  assert(two_loss.recover(&recovered, &recovered_len) == 2);
  assert(std::memcmp(recovered[0].data(), frames[kFirstMissing], sizeof(frames[0])) == 0);
  assert(std::memcmp(recovered[1].data(), frames[kSecondMissing], sizeof(frames[0])) == 0);

  fec_rs::Encoder partial_encoder;
  uint64_t partial_group_id = 99;
  fec_rs::Encoder::DataInfo partial_data{};
  fec_rs::Encoder::ParityInfo partial{};
  for (uint16_t i = 0; i < 3; ++i) {
    assert(!partial_encoder.add(frames[i], sizeof(frames[i]), &partial_group_id,
                                &partial_data, &partial));
    assert(partial_data.group_id == 99 && partial_data.shard_index == i);
  }
  assert(partial_encoder.flush(&partial));
  assert(partial.group_id == 99 && partial.shard_count == 3);
  assert(!partial_encoder.flush(&partial));
  printf("test_fec_rs_field_encoder_recovery_and_flush OK\n");
}

static void test_udp_deduper_window() {
  udp_deduper::Window window(8);
  assert(window.observe(100) == udp_deduper::Result::kAccept);
  assert(window.observe(102) == udp_deduper::Result::kAccept);
  assert(window.observe(101) == udp_deduper::Result::kAccept);  // bounded reorder
  assert(window.observe(101) == udp_deduper::Result::kDuplicate);
  assert(window.observe(110) == udp_deduper::Result::kAccept);
  assert(window.observe(103) == udp_deduper::Result::kAccept);  // oldest in window
  assert(window.observe(103) == udp_deduper::Result::kDuplicate);
  assert(window.observe(102) == udp_deduper::Result::kTooOld);
  printf("test_udp_deduper_window OK\n");
}

int main() {
  test_metrics_basic();
  test_metrics_drops();
  test_stage_metrics_tail_report();
  test_ring_roundtrip();
  test_ring_lapping();
  test_udp_wire_roundtrip_and_rejection();
  test_compact_wire_roundtrip();
  test_resequencer_order_and_deadline();
  test_fec_xor_single_loss_recovery();
  test_packet_fec_single_datagram_recovery();
  test_fec_rs_field_encoder_recovery_and_flush();
  test_udp_deduper_window();
  printf("ALL TESTS PASSED\n");
  return 0;
}
