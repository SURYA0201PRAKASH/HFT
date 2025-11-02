#pragma once
#include <atomic>
#include <map>
#include <mutex>
#include <chrono>
#include <functional>
#include <iostream>

class SequenceManager {
private:
    std::map<uint64_t, std::chrono::steady_clock::time_point> missing_sequences_;
    std::mutex sequence_mutex_;
    std::atomic<uint64_t> last_valid_sequence_{0};
    std::atomic<uint64_t> highest_received_sequence_{0};
    
    // Configuration
    std::chrono::milliseconds max_gap_tolerance_{1000}; // 1 second
    std::chrono::milliseconds retransmission_timeout_{5000}; // 5 seconds
    size_t max_retransmission_attempts_{3};
    
    // Statistics
    std::atomic<uint64_t> total_gaps_detected_{0};
    std::atomic<uint64_t> gaps_recovered_{0};
    std::atomic<uint64_t> gaps_failed_{0};

public:
    SequenceManager() {
        std::cout << "🔧 SequenceManager initialized" << std::endl;
        std::cout << "   Max gap tolerance: " << max_gap_tolerance_.count() << "ms" << std::endl;
        std::cout << "   Retransmission timeout: " << retransmission_timeout_.count() << "ms" << std::endl;
        std::cout << "   Max attempts: " << max_retransmission_attempts_ << std::endl;
    }
    
    void on_sequence_received(uint64_t sequence_number, 
                             std::function<void(uint64_t, uint64_t)> request_callback);
    
    void check_and_request_missing(std::function<void(uint64_t, uint64_t)> request_callback);
    
    void set_last_valid_sequence(uint64_t seq) { 
        last_valid_sequence_.store(seq, std::memory_order_release); 
    }
    
    uint64_t get_last_valid_sequence() const { 
        return last_valid_sequence_.load(std::memory_order_acquire); 
    }
    
    void print_stats();
    
    // Configuration setters
    void set_max_gap_tolerance(std::chrono::milliseconds tolerance) { 
        max_gap_tolerance_ = tolerance; 
    }
    
    void set_retransmission_timeout(std::chrono::milliseconds timeout) { 
        retransmission_timeout_ = timeout; 
    }
};