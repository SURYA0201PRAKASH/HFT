#include "sequence_manager.hpp"
#include <algorithm>

void SequenceManager::on_sequence_received(uint64_t sequence_number, 
                                          std::function<void(uint64_t, uint64_t)> request_callback) {
    uint64_t expected = last_valid_sequence_.load(std::memory_order_acquire);
    uint64_t highest = highest_received_sequence_.load(std::memory_order_acquire);
    
    // Update highest received sequence
    if (sequence_number > highest) {
        highest_received_sequence_.store(sequence_number, std::memory_order_release);
    }
    
    // Perfect sequence match (normal case)
    if (sequence_number == expected + 1) {
        last_valid_sequence_.store(sequence_number, std::memory_order_release);
        return;
    }
    
    // Sequence gap detected
    if (sequence_number > expected + 1) {
        uint64_t gap_size = sequence_number - expected - 1;
        total_gaps_detected_++;
        
        std::cout << "🚨 SEQUENCE GAP DETECTED: Expected " << expected + 1 
                  << ", Received " << sequence_number 
                  << ", Gap size: " << gap_size << " messages" << std::endl;
        
        // Add missing sequences to tracking
        std::lock_guard lock(sequence_mutex_);
        for (uint64_t missing = expected + 1; missing < sequence_number; ++missing) {
            if (missing_sequences_.find(missing) == missing_sequences_.end()) {
                missing_sequences_[missing] = std::chrono::steady_clock::now();
                std::cout << "   📋 Tracking missing sequence: " << missing << std::endl;
            }
        }
        
        // Immediately request small gaps
        if (gap_size <= 10) { // Small gap - request immediately
            std::cout << "   🔄 Requesting small gap immediately: " 
                      << (expected + 1) << " to " << (sequence_number - 1) << std::endl;
            request_callback(expected + 1, sequence_number - 1);
        }
        
        last_valid_sequence_.store(sequence_number, std::memory_order_release);
    }
    // Out-of-order message (older than expected)
    else if (sequence_number <= expected) {
        std::cout << "⚠️ OUT-OF-ORDER MESSAGE: Received " << sequence_number 
                  << ", Expected > " << expected 
                  << " (Ignoring duplicate/old message)" << std::endl;
    }
}

void SequenceManager::check_and_request_missing(std::function<void(uint64_t, uint64_t)> request_callback) {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(sequence_mutex_);
    
    std::vector<uint64_t> to_remove;
    std::vector<uint64_t> to_request;
    
    for (auto& [seq, detection_time] : missing_sequences_) {
        auto time_since_detection = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - detection_time);
        
        // Check if this sequence should be requested
        if (time_since_detection > retransmission_timeout_) {
            to_request.push_back(seq);
        }
        
        // Remove if too old (failed recovery)
        if (time_since_detection > retransmission_timeout_ * max_retransmission_attempts_) {
            to_remove.push_back(seq);
            gaps_failed_++;
            std::cout << "❌ GAP RECOVERY FAILED for sequence: " << seq 
                      << " (timeout: " << time_since_detection.count() << "ms)" << std::endl;
        }
    }
    
    // Request missing sequences in batches
    if (!to_request.empty()) {
        std::sort(to_request.begin(), to_request.end());
        uint64_t range_start = to_request.front();
        uint64_t range_end = to_request.front();
        
        for (size_t i = 1; i < to_request.size(); ++i) {
            if (to_request[i] == range_end + 1) {
                range_end = to_request[i];
            } else {
                // Request current range
                std::cout << "🔄 REQUESTING MISSING SEQUENCES: " 
                          << range_start << " to " << range_end << std::endl;
                request_callback(range_start, range_end);
                
                range_start = range_end = to_request[i];
            }
        }
        
        // Request final range
        std::cout << "🔄 REQUESTING MISSING SEQUENCES: " 
                  << range_start << " to " << range_end << std::endl;
        request_callback(range_start, range_end);
        
        gaps_recovered_ += to_request.size();
    }
    
    // Remove failed sequences
    for (uint64_t seq : to_remove) {
        missing_sequences_.erase(seq);
    }
}

void SequenceManager::print_stats() {
    std::lock_guard lock(sequence_mutex_);
    std::cout << "\n📊 SEQUENCE MANAGER STATS:" << std::endl;
    std::cout << "   Last valid sequence: " << last_valid_sequence_.load() << std::endl;
    std::cout << "   Highest received: " << highest_received_sequence_.load() << std::endl;
    std::cout << "   Currently tracking: " << missing_sequences_.size() << " missing sequences" << std::endl;
    std::cout << "   Total gaps detected: " << total_gaps_detected_.load() << std::endl;
    std::cout << "   Gaps recovered: " << gaps_recovered_.load() << std::endl;
    std::cout << "   Gaps failed: " << gaps_failed_.load() << std::endl;
    
    if (!missing_sequences_.empty()) {
        std::cout << "   Missing sequences: ";
        for (const auto& [seq, time] : missing_sequences_) {
            auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - time);
            std::cout << seq << "(" << age.count() << "ms) ";
        }
        std::cout << std::endl;
    }
}