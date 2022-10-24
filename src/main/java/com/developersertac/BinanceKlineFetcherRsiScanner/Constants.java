package com.developersertac.BinanceKlineFetcherRsiScanner;

import java.util.Calendar;
import java.util.Date;

import lombok.Getter;

@Getter
public final class Constants {
	public static final long uts1m = 60000;
	public static final long uts1h = uts1m * 60;
	public static final long uts1d = uts1h * 24;
	public static final long uts1M = uts1d * 30;
	public static final long uts1y = uts1M * 12;
	public static final long uts3y = uts1y * 3;
	
	public static long truncByInterval(long utsMs, Interval interval) {
		long utsMsOut = utsMs;
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date(utsMs));
		
		switch (interval.getCode()) {
			case "3d":
				utsMsOut -= (utsMsOut % Interval._1d.getUtsMs());
				break;
			case "1w":
				utsMsOut -= (utsMsOut % Interval._1d.getUtsMs()) + ((calendar.get(Calendar.DAY_OF_WEEK) + 5) % 7) * Constants.uts1d;
				break;
			case "1M":
				utsMsOut -= (utsMsOut % Interval._1d.getUtsMs()) + (calendar.get(Calendar.DAY_OF_MONTH) - 1) * Constants.uts1d;
				break;
			default:
				utsMsOut -= (utsMsOut % interval.getUtsMs());
		}
		
		return utsMsOut;
	}
}
