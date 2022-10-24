package com.developersertac.BinanceKlineFetcherRsiScanner;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class RsiData {
	
	private MongoOperations mongoOps;

	private KlineData klineData;
	
	@Getter
	@NoArgsConstructor
	private class RsiGainLoss {

		BigDecimal gain;
		BigDecimal loss;
		
		public void initGainLoss(BigDecimal change) {

			this.gain = BigDecimal.ZERO;
			this.loss = BigDecimal.ZERO;
			
			if (change.compareTo(BigDecimal.ZERO) > 0) {

				this.gain = change;
				this.loss = BigDecimal.ZERO;
				
			}
			else {

				this.gain = BigDecimal.ZERO;
				this.loss = change.negate();
				
			}
		}
		
	}

	@AllArgsConstructor
	public class RsiTask implements Runnable {
		private String symbol;
		private Interval interval;
		private Integer period;
		
		private CountDownLatch countDownLatch;

		@Override
		public void run() {

			Long duration = System.currentTimeMillis();
			
			updateRSI(symbol, interval, period);
			
			countDownLatch.countDown();
			
			duration = System.currentTimeMillis() - duration;
			
			Core.logL(symbol + "-" + interval.getCode() + "-RSI(" + period + ") " + "update duration: " + (duration / (1000 * 60)) + " minutes " + ((duration / 1000) % 60) + " seconds " + (duration % 1000) + " milliseconds" + " countDownLatch: " + countDownLatch.getCount());
		}
	}

	public Runnable RsiTask(String symbol, Interval interval, Integer period, CountDownLatch countDownLatch) {
		
		return new RsiTask(symbol, interval, period, countDownLatch);
	}

	public RsiData(Globals globals) {
    	
		this.mongoOps = globals.getMongoOps();

		this.klineData = new KlineData(globals);
		
	}
	
	public Rsi getDocumentWithMaxValueOf(String collectionName, String key) {
		
		return getFirstDocumentSortedBy(collectionName, key, Sort.Direction.DESC);
		
	}
	
	public Rsi getFirstDocumentSortedBy(String collectionName, String key, Direction sort) {
		
		List<Rsi> klineList = getFirstNDocumentSortedBy(collectionName, key, sort, 1);
		
		if (klineList.isEmpty()) return new Rsi();
		
		return klineList.get(0);
		
	}
	
	public List<Rsi> getFirstNDocumentSortedBy(String collectionName, String key, Direction sort, int limit) {
		
		if (mongoOps.getCollection(collectionName).countDocuments() > 0) {
		
			return mongoOps.find(
					new Query().with(Sort.by(sort, key)).limit(limit), 
					Rsi.class, 
					collectionName);
			
		}
		
		return Collections.<Rsi>emptyList();
		
	}
	
	public Rsi getDocumentWithOpenTime(String collectionName, Long openTime) {
		
		return mongoOps.findOne(
				new Query().addCriteria(Criteria.where("openTime").is(openTime)), 
				Rsi.class, 
				collectionName);
		
	}
	
	private BigDecimal calculateRsi(BigDecimal averageGain, BigDecimal averageLoss) {
		
		BigDecimal Rs = averageGain.divide(averageLoss, 8, RoundingMode.HALF_EVEN);
		BigDecimal _100 = new BigDecimal("100");
		
		return _100.subtract(_100.divide(BigDecimal.ONE.add(Rs), 8, RoundingMode.HALF_EVEN));
		
	}
	
	public void updateRSI(String symbol, Interval interval, Integer period) {

		String parityCollName = symbol + "-" + interval.getCode();
		String rsiCollName = symbol + "-" + interval.getCode() + "-RSI(" + period + ")";
		
		Long startTime = 0L;
		Long endTime = 0L;
		RsiGainLoss rGL = new RsiGainLoss();
		BigDecimal averageGain = BigDecimal.ZERO;
		BigDecimal averageLoss = BigDecimal.ZERO;
		Long currentTime = 0L;
		BigDecimal previousClose = BigDecimal.ZERO;
		BigDecimal currentClose = BigDecimal.ZERO;
		BigDecimal divisor = new BigDecimal(period);
		
		if (this.mongoOps.getCollection(rsiCollName).countDocuments() == 0) {
			
			if (this.mongoOps.getCollection(parityCollName).countDocuments() > period) {
				
				List<BinanceKline> klineList = this.klineData.getFirstNDocumentSortedBy(parityCollName, "openTime", Sort.Direction.ASC, period + 1);
				
				for(int i = 1; i < klineList.size(); i++) {
					
					rGL.initGainLoss(klineList.get(i).getClose().subtract(klineList.get(i-1).getClose()));
					averageGain = averageGain.add(rGL.getGain());
					averageLoss = averageLoss.add(rGL.getLoss());
					
				}
				
				averageGain = averageGain.divide(divisor, 8, RoundingMode.HALF_EVEN);
				averageLoss = averageLoss.divide(divisor, 8, RoundingMode.HALF_EVEN);

				Long firstRsiOpenTime = klineList.get(klineList.size() - 1).getOpenTime();
				
				this.mongoOps.insert(new Rsi(firstRsiOpenTime, averageGain, averageLoss, calculateRsi(averageGain, averageLoss)), rsiCollName);
				
			}
			else {
				
				Core.logL("--------------------------\nNot enough klines for RSI calculation of " + rsiCollName + "\n--------------------------");
				
				return;
			}
			
		}
		
		this.mongoOps.indexOps(rsiCollName).ensureIndex(new Index().on("openTime", Direction.ASC));

		startTime = this.getDocumentWithMaxValueOf(rsiCollName, "openTime").getOpenTime();
		endTime = this.klineData.getDocumentWithMaxValueOf(parityCollName, "openTime").getOpenTime();
		currentTime = startTime + interval.getUtsMs();
		previousClose = this.klineData.getDocumentWithOpenTime(parityCollName, startTime).getClose();
		averageGain = this.getDocumentWithOpenTime(rsiCollName, startTime).getAverageGain();
		averageLoss = this.getDocumentWithOpenTime(rsiCollName, startTime).getAverageLoss();
				
		while(currentTime <= endTime) {
			
			try {
				
				currentClose = this.klineData.getDocumentWithOpenTime(parityCollName, currentTime).getClose();
				
			} catch (NullPointerException e) {

				currentTime += interval.getUtsMs();
				previousClose = currentClose;
				
				continue;
			}
			
			rGL.initGainLoss(currentClose.subtract(previousClose));
			averageGain = averageGain.multiply(new BigDecimal(period - 1)).add(rGL.getGain()).divide(divisor, 8, RoundingMode.HALF_EVEN);
			averageLoss = averageLoss.multiply(new BigDecimal(period - 1)).add(rGL.getLoss()).divide(divisor, 8, RoundingMode.HALF_EVEN);
			
			this.mongoOps.insert(new Rsi(currentTime, averageGain, averageLoss, calculateRsi(averageGain, averageLoss)), rsiCollName);
			
			currentTime += interval.getUtsMs();
			previousClose = currentClose;
		}
		
		String lastRsi = this.getDocumentWithOpenTime(rsiCollName, endTime).getRSI().divide(BigDecimal.ONE, 2, RoundingMode.HALF_EVEN).toEngineeringString();
		
		Core.logL("--------------------------\nLast RSI for " + rsiCollName + ": " + lastRsi + "\n--------------------------");
		
	}

}
