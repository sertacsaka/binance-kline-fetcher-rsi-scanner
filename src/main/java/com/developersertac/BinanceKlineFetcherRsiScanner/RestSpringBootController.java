package com.developersertac.BinanceKlineFetcherRsiScanner;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.MongoClient;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@RestController
public class RestSpringBootController implements InitializingBean {
	
	@Autowired
	Globals globals;

	private KlineData klineData;
	private RsiData rsiData;
	private MongoClient mongoClient;
	private MongoOperations mongoOps;
	
	DateFormat dateFormat;

	@Getter
	@Setter
	@NoArgsConstructor
	private class RsiStatus {

		private String symbol;
		private Long lastOpenTime;
		private BigDecimal lastClose;
		private BigDecimal previousClose;
		private Double lastRsi;
		private Double previousRsi;
		private String signal;
		
	}
	
	public String rsiSignal(Double previousRsi, Double lastRsi) {
		
		if (previousRsi < 30 && lastRsi > 30) return "Buy";
		if (previousRsi > 70 && lastRsi < 70) return "Sell";
		if (previousRsi < 70 && lastRsi > 70) return "Just Over Bought";
		if (previousRsi > 30 && lastRsi < 30) return "Just Over Sold";
		if (previousRsi > 70 && lastRsi > 70) return "Still Over Bought";
		if (previousRsi < 30 && lastRsi < 30) return "Still Over Sold";
		if (previousRsi > 30 && previousRsi < 70 && lastRsi > 30 && lastRsi < 70) return "Wait";
		
		return null;
		
	}

	@GetMapping("/rsi-scan")
	public String rsiScan(@RequestParam String interval) {
		
		String list = "";
		List<RsiStatus> rsiStatus = new ArrayList<RsiStatus>();
		
		for(String collName : mongoClient.getDatabase(globals.springDataMongoDb().getDatabase()).listCollectionNames()) {
			
			if (collName.contains("-" + interval + "-RSI(")) {
				
				RsiStatus rs = new RsiStatus();

				rs.setSymbol(collName.split("-")[0]);
				
				String parityCollName = rs.getSymbol() + "-" + interval;
				
				List<Rsi> r = this.rsiData.getFirstNDocumentSortedBy(collName, "openTime", Sort.Direction.DESC, 2);
				
				rs.setLastOpenTime(r.get(1).getOpenTime());
				
				List<BinanceKline> bK = this.klineData.getFirstNDocumentSortedBy(parityCollName, "openTime", Sort.Direction.DESC, 2);
				
				rs.setLastClose(bK.get(1).getClose());
				rs.setPreviousClose(bK.get(0).getClose());
				
				rs.setLastRsi(r.get(1).getRSI().divide(BigDecimal.ONE, 2, RoundingMode.HALF_EVEN).doubleValue());
				rs.setPreviousRsi(r.get(0).getRSI().divide(BigDecimal.ONE, 2, RoundingMode.HALF_EVEN).doubleValue());
				
				rs.setSignal(rsiSignal(rs.getPreviousRsi(), rs.getLastRsi()));
				
				rsiStatus.add(rs);
				
			}
		}

		list += "<table border=1>";
		list += "<tr style=\"font-weight:bold\"><td colspan=7>RSI Scan Results For " + interval + " Interval</td></tr>";
		list += "<tr style=\"font-weight:bold\"><td>Symbol</td><td>Last Open Time</td><td>Previous Close</td><td>Last Close</td><td>Previous Rsi</td><td>Last Rsi</td><td>Signal</td></tr>";
		
		for(RsiStatus r : rsiStatus) {

			list += "<tr>";
			list += "<td>" + r.getSymbol() + "</td>";
			list += "<td>" + dateFormat.format(new Date(r.getLastOpenTime())) + "</td>";
			list += "<td>" + r.getPreviousClose() + "</td>";
			list += "<td>" + r.getLastClose() + "</td>";
			list += "<td>" + r.getPreviousRsi() + "</td>";
			list += "<td>" + r.getLastRsi() + "</td>";
			list += "<td>" + r.getSignal() + "</td>";
			list += "</tr>";
			
		}
		
		list += "</table>";
		
		return list;
		
	}

	@GetMapping("/kline-statistics")
	public String klineStatistics(@RequestParam String symbol, @RequestParam String interval) {
		
		String description = "";
		String collectionName = symbol + "-" + interval;

		String latestOpenPrice = klineData.getDocumentWithMaxValueOf(collectionName, "openTime").getOpen().toEngineeringString();
		long lastOpenTimeUtsMs = klineData.getDocumentWithMaxValueOf(collectionName, "openTime").getOpenTime();
		String lastOpenTime = dateFormat.format(new Date(lastOpenTimeUtsMs));
		String firstOpenTime = dateFormat.format(new Date(klineData.getDocumentWithMinValueOf(collectionName, "openTime").getOpenTime()));
		String minCloseValue = klineData.getDocumentWithMinValueOf(collectionName, "close").getClose().toEngineeringString();
		String maxCloseValue = klineData.getDocumentWithMaxValueOf(collectionName, "close").getClose().toEngineeringString();
		String klineCount = String.valueOf(mongoOps.getCollection(collectionName).countDocuments());

		description += "<h2>" + symbol + " Parity Statistics for " + interval + " interval</h2>";
		description += "<ul>";
		description += "<li><b>Latest open price:</b> " + latestOpenPrice + "</li>";
		description += "<li><b>Last open time:</b> " + lastOpenTime + "</li>";
		description += "<li><b>First open time:</b> " + firstOpenTime + "</li>";
		description += "<li><b>Minimum close value:</b> " + minCloseValue + "</li>";
		description += "<li><b>Maximum close value:</b> " + maxCloseValue + "</li>";
		description += "<li><b>Kline count:</b> " + klineCount + "</li>";
		description += "</ul>";
		
		description += "<br>";

		description += "<h2>RSI Info</h2>";
		
		for(String collName : mongoClient.getDatabase(globals.springDataMongoDb().getDatabase()).listCollectionNames()) {

			if (collName.startsWith(collectionName + "-RSI")) {

				description += "<ul>";

				String rsiPeriod = collName.substring(collName.indexOf("RSI(") + 4, collName.indexOf(")"));
				
				String lastRsi = rsiData.getDocumentWithOpenTime(collName, lastOpenTimeUtsMs).getRSI().divide(BigDecimal.ONE, 2, RoundingMode.HALF_EVEN).toEngineeringString();
				String previousRsi = rsiData.getDocumentWithOpenTime(collName, lastOpenTimeUtsMs - klineData.getIntervalFromCode(interval).getUtsMs()).getRSI().divide(BigDecimal.ONE, 2, RoundingMode.HALF_EVEN).toEngineeringString();

				description += "<li><b>RSI period:</b> " + rsiPeriod + "</li>";
				description += "<li><b>Previous RSI:</b> " + previousRsi + "</li>";
				description += "<li><b>Last RSI:</b> " + lastRsi + "</li>";
				
				description += "<li><b>Signal:</b> " + rsiSignal(Double.valueOf(previousRsi), Double.valueOf(lastRsi)) + "</li>";
				description += "</ul>";
				
			}
			
		}
		
		return description;
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		this.klineData = new KlineData(globals);
		this.rsiData = new RsiData(globals);
		this.mongoClient = globals.getMongoClient();
		this.mongoOps = globals.getMongoOps();
		this.dateFormat = globals.getDateFormat();
		
	}
}
