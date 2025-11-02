#include "tick_data.hpp"
#include <iostream>

TickBuffer::TickBuffer(size_t size) : capacity_(size) {
    buffer_.resize(size);
}

bool TickBuffer::push(const Tick& tick) {
    size_t current_tail = tail_.load(std::memory_order_acquire);
    size_t next_tail = (current_tail + 1) % capacity_;
    
    if (next_tail == head_.load(std::memory_order_acquire)) {
        full_.store(true, std::memory_order_release);
        return false;
    }
    
    buffer_[current_tail] = tick;
    tail_.store(next_tail, std::memory_order_release);
    full_.store(false, std::memory_order_release);
    return true;
}

bool TickBuffer::pop(Tick& tick) {
    if (empty()) return false;
    
    size_t current_head = head_.load(std::memory_order_acquire);
    tick = buffer_[current_head];
    head_.store((current_head + 1) % capacity_, std::memory_order_release);
    full_.store(false, std::memory_order_release);
    return true;
}

bool TickBuffer::empty() const {
    return head_.load(std::memory_order_acquire) == 
           tail_.load(std::memory_order_acquire) && 
           !full_.load(std::memory_order_acquire);
}

size_t TickBuffer::size() const {
    size_t head = head_.load(std::memory_order_acquire);
    size_t tail = tail_.load(std::memory_order_acquire);
    
    if (tail >= head) return tail - head;
    return capacity_ - head + tail;
}
