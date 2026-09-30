# Standalone build for the fan-out benchmark harness. No external dependencies
# beyond a C++17 compiler and librt (POSIX shared memory).
CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -D_GNU_SOURCE -Iinclude
LDLIBS   ?= -lrt -lpthread

BIN := bin
BINARIES := $(BIN)/producer $(BIN)/consumer $(BIN)/udp_sender $(BIN)/udp_receiver

.PHONY: all clean test bench-codec test-integration test-fanout-integration test-fec-integration

all: $(BINARIES)

$(BIN):
	mkdir -p $(BIN)

$(BIN)/producer: src/producer.cpp include/*.h | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS)

$(BIN)/consumer: src/consumer.cpp include/*.h | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS)

$(BIN)/udp_sender: src/udp_sender.cpp include/*.h | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS)

$(BIN)/udp_receiver: src/udp_receiver.cpp include/*.h | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS)

test: $(BIN)/test_harness
	./$(BIN)/test_harness

$(BIN)/test_harness: test/test_harness.cpp include/*.h | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS)

bench-codec: $(BIN)/bench_compact_codec
	./$(BIN)/bench_compact_codec

$(BIN)/bench_compact_codec: bench/bench_compact_codec.cpp include/*.h | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS)

test-integration: all
	WIRE_FORMAT=full bash ./test/test_udp_integration.sh
	WIRE_FORMAT=compact bash ./test/test_udp_integration.sh
	UDP_MODE=connected WIRE_FORMAT=full bash ./test/test_udp_integration.sh
	FEC_MODE=rs8_2 COUNT=17 WIRE_FORMAT=full bash ./test/test_udp_integration.sh
	BATCH_BYTES=1472 RATE=10000 COUNT=100 WIRE_FORMAT=full bash ./test/test_udp_integration.sh
	FEC_MODE=xor4 BATCH_BYTES=1472 RATE=10000 COUNT=100 WIRE_FORMAT=full bash ./test/test_udp_integration.sh
	FEC_MODE=rs4_2 BATCH_BYTES=1472 RATE=10000 COUNT=100 WIRE_FORMAT=full bash ./test/test_udp_integration.sh
	FEC_MODE=xor4 BATCH_BYTES=1472 RATE=10000 COUNT=100 WIRE_FORMAT=compact bash ./test/test_udp_integration.sh
	FEC_MODE=rs4_2 BATCH_BYTES=1472 RATE=10000 COUNT=100 WIRE_FORMAT=compact bash ./test/test_udp_integration.sh
	FEC_MODE=packet_xor4 BATCH_BYTES=1472 RATE=10000 COUNT=100 WIRE_FORMAT=compact bash ./test/test_udp_integration.sh

test-fanout-integration: all
	FANOUT_MODE=loop bash ./test/test_udp_fanout_integration.sh
	FANOUT_MODE=sendmmsg bash ./test/test_udp_fanout_integration.sh
	UDP_MODE=connected FANOUT_MODE=loop bash ./test/test_udp_fanout_integration.sh
	SENDER_WORKERS=auto UDP_MODE=connected FANOUT_MODE=loop bash ./test/test_udp_fanout_integration.sh
	FEC_MODE=xor4 UDP_MODE=connected FANOUT_MODE=loop bash ./test/test_udp_fanout_integration.sh

test-fec-integration: all
	bash ./test/test_fec_integration.sh

clean:
	rm -rf $(BIN)
