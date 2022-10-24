package com.developersertac.BinanceKlineFetcherRsiScanner;

import lombok.Getter;

@Getter
public enum Interval {
    _1m("1m", 1 * Constants.uts1m),
    _3m("3m", 3 * Constants.uts1m),
    _5m("5m", 5 * Constants.uts1m),
    _15m("15m", 15 * Constants.uts1m),
    _30m("30m", 30 * Constants.uts1m),
    _1h("1h", 1 * Constants.uts1h),
    _2h("2h", 2 * Constants.uts1h),
    _4h("4h", 4 * Constants.uts1h),
    _6h("6h", 6 * Constants.uts1h),
    _8h("8h", 8 * Constants.uts1h),
    _12h("12h", 12 * Constants.uts1h),
    _1d("1d", 1 * Constants.uts1d),
    _3d("3d", 3 * Constants.uts1d),
    _1w("1w", 7 * Constants.uts1d),
    _1M("1M", 30 * Constants.uts1d); 
	
    private final String code;
    private long utsMs;

    Interval(String code, long utsMs) {
        this.code = code;
        this.utsMs = utsMs;
    }
}