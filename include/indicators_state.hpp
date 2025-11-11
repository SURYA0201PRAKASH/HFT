#pragma once
#include <deque>
#include <string>
#include <unordered_map>
#include <cmath>

struct Indicators {
    double ema12=0, ema26=0, macd=0;
    double rsi14=0;
    double bb_mid=0, bb_up=0, bb_low=0;
    double vwap=0;
    double mid=0, spread=0;
};

struct IndicatorState {
    bool initialized=false;
    double ema12=0, ema26=0;
    double prev_price=0;
    double avg_gain=0, avg_loss=0;
    int rsi_count=0;

    std::deque<double> bb_win;
    double sum=0, sumsq=0;
    long double cum_pv=0, cum_vol=0;

    static constexpr double k12=2.0/(12.0+1.0);
    static constexpr double k26=2.0/(26.0+1.0);
    static constexpr int bb_period=20;
    static constexpr double bb_k=2.0;
};

inline Indicators update_indicators(IndicatorState& s, double mid, double trade_price, double trade_qty)
{
    Indicators o{};
    if(!s.initialized){ s.ema12=mid; s.ema26=mid; s.prev_price=mid; s.initialized=true; }

    s.ema12 += IndicatorState::k12*(mid - s.ema12);
    s.ema26 += IndicatorState::k26*(mid - s.ema26);
    o.ema12=s.ema12; o.ema26=s.ema26; o.macd=s.ema12 - s.ema26;

    double chg=mid - s.prev_price;
    double gain=chg>0?chg:0, loss=chg<0?-chg:0;
    if(s.rsi_count<14){
        s.avg_gain+=gain; s.avg_loss+=loss; s.rsi_count++;
        if(s.rsi_count==14){ s.avg_gain/=14; s.avg_loss/=14; }
        o.rsi14=0;
    } else {
        s.avg_gain=(s.avg_gain*13+gain)/14;
        s.avg_loss=(s.avg_loss*13+loss)/14;
        double rs=(s.avg_loss<=1e-12)?1e6:(s.avg_gain/s.avg_loss);
        o.rsi14=100.0 - (100.0/(1.0+rs));
    }
    s.prev_price=mid;

    s.bb_win.push_back(mid);
    s.sum+=mid; s.sumsq+=mid*mid;
    if((int)s.bb_win.size()>IndicatorState::bb_period){
        double old=s.bb_win.front(); s.bb_win.pop_front();
        s.sum-=old; s.sumsq-=old*old;
    }
    int n=s.bb_win.size();
    double mean=s.sum/n;
    double var=std::max(0.0, (s.sumsq/n) - mean*mean);
    double stddev=std::sqrt(var);
    o.bb_mid=mean; o.bb_up=mean+IndicatorState::bb_k*stddev; o.bb_low=mean-IndicatorState::bb_k*stddev;

    if(trade_qty>0){ s.cum_pv += (long double)trade_price*trade_qty; s.cum_vol += trade_qty; }
    o.vwap=(s.cum_vol>0)?(double)(s.cum_pv/s.cum_vol):trade_price;

    o.mid=mid;
    return o;
}
