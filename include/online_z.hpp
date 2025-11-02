#pragma once
#include <cmath>
#include <algorithm>

// Exponential moving Z-score normalizer (online, constant time).
// Use a small alpha (e.g., 0.001) for stability in HFT streams.
struct OnlineZ {
    double mean{0.0};
    double var{1.0};
    double alpha{0.001};   // smoothing; from your YAML (features.zscore_alpha)

    OnlineZ() = default;
    explicit OnlineZ(double a) : mean(0.0), var(1.0), alpha(a) {}

    // Update with new observation x and return its z-score.
    inline double update(double x) {
        // EMA mean/variance (Welford-style with EMA)
        double diff = x - mean;
        mean += alpha * diff;
        // keep var positive; blend old var with squared deviation
        var  = (1.0 - alpha) * var + alpha * diff * diff;

        // numerical guard
        double denom = std::sqrt(std::max(var, 1e-12));
        return (x - mean) / denom;
    }

    // Optionally peek z-score without mutating internal state
    inline double z_peek(double x) const {
        double diff = x - mean;
        double denom = std::sqrt(std::max(var, 1e-12));
        return diff / denom;
    }

    // Reset stats (e.g., on session roll)
    inline void reset(double m = 0.0, double v = 1.0) {
        mean = m; var = std::max(v, 1e-12);
    }
};
