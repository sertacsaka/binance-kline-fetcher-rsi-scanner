package com.developersertac.BinanceKlineFetcherRsiScanner;

import java.text.DateFormat;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.result.DeleteResult;

public class KlineData {
	
	private MongoOperations mongoOps;
	
	private DateFormat dateFormat;

	private String baseEndpoint;
	private String klinesEndpoint;
	private String timeEndpoint;
	private String exchangeinfoEndpoint;
	private int binanceLimit;
    
	private boolean firstFetchAllInsertLater;
	private boolean loggingOn;
	private boolean deleteBeforeInsert;

	private String parityBase;
	
	private Long exchangeTimeDifference;

	public KlineData(Globals globals) {
    	
		this.mongoOps = globals.getMongoOps();
    	
		this.dateFormat = globals.getDateFormat();
		
		this.loggingOn = globals.flow().isLoggingOn();
		this.firstFetchAllInsertLater = globals.flow().isFirstFetchAllInsertLater();
		this.deleteBeforeInsert = globals.flow().isDeleteBeforeInsert();

		this.baseEndpoint = globals.binance().getBaseEndpoint();
		this.klinesEndpoint = globals.binance().getKlinesEndpoint();
		this.timeEndpoint = globals.binance().getTimeEndpoint();
		this.exchangeinfoEndpoint = globals.binance().getExchangeinfoEndpoint();
		this.binanceLimit = globals.binance().getLimit();

		this.parityBase = globals.parity().getBase();

		this.exchangeTimeDifference = getServerTime(true) - System.currentTimeMillis();
	}

	public String getKlineArrayJsonString(String symbol, Interval interval, Long startTime, Long endTime) {
		
		String uri = this.baseEndpoint + this.klinesEndpoint + "?symbol=" + symbol + "&interval=" + interval.getCode() + "&limit=" + binanceLimit;
		
		if (startTime != 0) 
		{
			uri += "&startTime=" + Long.toString(startTime);
			
			if (endTime != 0) 
			{
				uri += "&endTime=" + Long.toString(endTime);
			}
		}

		return (new RestTemplate()).getForObject(uri, String.class);
	}
	
	public List<BinanceKline> convertArrayJsonStringToKlineList(String json) {
		
		if (!json.isBlank() && !json.isEmpty()) {

			try {
				
				List<BinanceKline> binanceKlineList = new ArrayList<BinanceKline>();
				
				for(String[] s : (new ObjectMapper()).readValue(json, new TypeReference<List<String[]>>(){})) 
					binanceKlineList.add(new BinanceKline(s));
				
				return binanceKlineList;
				
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			
		}
		
		return Collections.<BinanceKline>emptyList();
	}
	
	public List<BinanceKline> getKlineList(String symbol, Interval interval, Long startTime, Long endTime) {
		
		return this.convertArrayJsonStringToKlineList(this.getKlineArrayJsonString(symbol, interval, startTime, endTime));
	}

	public boolean klineExists(String symbol, Interval interval, Long openTime) {
		
		List<BinanceKline> kline = this.getKlineList(symbol, interval, openTime, openTime + interval.getUtsMs());

		if ( kline.isEmpty() || ( !kline.isEmpty() && kline.size() == 0 ) ) return false;
		
		return true;
	}
	
	public Long getServerTime(boolean fromServer) {
		
		if (fromServer)
			return (new JSONObject((new RestTemplate()).getForObject(this.baseEndpoint + this.timeEndpoint, String.class))).getLong("serverTime");

		return System.currentTimeMillis() + this.exchangeTimeDifference;
		
	};
	
	public Long openTimeOfUncompletedKline(Interval interval) {
		return Constants.truncByInterval(getServerTime(false), interval); 
	};
	
	public String concatenateTwoKlineArrayJsonStrings(String klineArrayJsonString1, String klineArrayJsonString2) {

		if (klineArrayJsonString1.isEmpty() && !klineArrayJsonString2.isEmpty()) return klineArrayJsonString2;
		if (!klineArrayJsonString1.isEmpty() && klineArrayJsonString2.isEmpty()) return klineArrayJsonString1;
		if (klineArrayJsonString1.isEmpty() && klineArrayJsonString2.isEmpty()) return "";
		
		return klineArrayJsonString1.substring(0, klineArrayJsonString1.length() - 1) + "," + klineArrayJsonString2.substring(1);
		
	}

	public List<BinanceKline> fetchKlineList(String symbol, Interval interval, Long startTime) {

		Core.logL("\t\tFetch kline array list from startTime: " + startTime + " = " + this.dateFormat.format(startTime));
		
		String klines = "";
		Long stepBlockSize = this.binanceLimit * interval.getUtsMs();
		Long lastCompletedKlineOpenTime = 0L;
		Long newUncompletedKlineOpenTime = 0L;
		Long uncompletedKlineOpenTime = 0L;
		Long openTime = startTime;
		Long endTime = 0L;
		int apiCallCount = 0;

		do {
			
			newUncompletedKlineOpenTime = this.openTimeOfUncompletedKline(interval);
			
			if (uncompletedKlineOpenTime < newUncompletedKlineOpenTime) {
				
				uncompletedKlineOpenTime = newUncompletedKlineOpenTime;
				lastCompletedKlineOpenTime = uncompletedKlineOpenTime - interval.getUtsMs();

				while (openTime < lastCompletedKlineOpenTime) {

					endTime = openTime + stepBlockSize;

					if (endTime >= lastCompletedKlineOpenTime) 
						endTime = lastCompletedKlineOpenTime;

					klines = this.concatenateTwoKlineArrayJsonStrings(klines, this.getKlineArrayJsonString(symbol, interval, openTime, endTime));
				
					openTime = endTime;
					
					apiCallCount++;
					
				}
				
			}
			else
				break;
			
		} while (true);
		
		Core.logL("\t\t\tDone! API called " + apiCallCount + " times.");
		
		return this.convertArrayJsonStringToKlineList(klines);
	}

	public void fetchAndInsertKlines(String symbol, Interval interval, Long startTime, String collectionName) {

		Core.logL("\t\tFetch and insert klines from startTime: " + startTime + " = " + this.dateFormat.format(startTime));
		
		List<BinanceKline> klineList = null;
		Long stepBlockSize = this.binanceLimit * interval.getUtsMs();
		Long lastCompletedKlineOpenTime = 0L;
		Long uncompletedKlineOpenTime = 0L;
		Long openTime = startTime;
		Long endTime = 0L;
		int apiCallCount = 0;
		
		if (startTime >= this.openTimeOfUncompletedKline(interval)) {
			
			Core.logL("\t\tNo new completed klines to insert");
			
		}
		else {
			
			if (this.deleteBeforeInsert) {

				DeleteResult deleteResult = this.mongoOps.remove(new Query(Criteria.where("openTime").gte(startTime)), collectionName);
	
				Core.logL("\t" + deleteResult.getDeletedCount() + " documents deleted.");
			
			}

			do {

				uncompletedKlineOpenTime = this.openTimeOfUncompletedKline(interval);
				lastCompletedKlineOpenTime = uncompletedKlineOpenTime - interval.getUtsMs();
				
				do {

					endTime = openTime + stepBlockSize;

					if (endTime > lastCompletedKlineOpenTime) 
						endTime = lastCompletedKlineOpenTime;

					Core.log("\t\t\tFrom " + openTime + " (" + this.dateFormat.format(openTime) + ") ");
					Core.logL("to " + endTime + " (" + this.dateFormat.format(endTime) + ") ");

					klineList = this.convertArrayJsonStringToKlineList(this.getKlineArrayJsonString(symbol, interval, openTime, endTime));
					
					Collection<BinanceKline> insertedObjects = this.mongoOps.insert(klineList, collectionName);
					
					if (insertedObjects.isEmpty())
						
						Core.logL("\tEmpty array of inserted objects for " + klineList.size() + " documents");
					
					else
						
						Core.logL("\t" + insertedObjects.size() + " objects inserted");
				
					openTime = endTime;
					
					if (this.loggingOn) apiCallCount++;
					
				} while (openTime < lastCompletedKlineOpenTime);
				
			} while (uncompletedKlineOpenTime < openTimeOfUncompletedKline(interval));
			
		}

		Core.logL("\t\tDocuments count: " + this.mongoOps.getCollection(collectionName).countDocuments());
		
		if (this.loggingOn) {
			
			Long maxOpenTime = this.getDocumentWithMaxValueOf(collectionName, "openTime").getOpenTime();
		
			Core.logL("\t\t\tLatest openTime in DB: " + maxOpenTime + " = " + this.dateFormat.format(maxOpenTime));
			
		}
		
		Core.logL("\t\tDone! API called " + apiCallCount + " times.");
	}
	
	public BinanceKline getDocumentWithMinValueOf(String collectionName, String key) {
		
		return getFirstDocumentSortedBy(collectionName, key, Sort.Direction.ASC);
		
	}
	
	public BinanceKline getDocumentWithMaxValueOf(String collectionName, String key) {
		
		return getFirstDocumentSortedBy(collectionName, key, Sort.Direction.DESC);
		
	}
	
	public BinanceKline getFirstDocumentSortedBy(String collectionName, String key, Direction sort) {
		
		List<BinanceKline> klineList = getFirstNDocumentSortedBy(collectionName, key, sort, 1);
		
		if (klineList.isEmpty()) return new BinanceKline();
		
		return klineList.get(0);
		
	}
	
	public List<BinanceKline> getFirstNDocumentSortedBy(String collectionName, String key, Direction sort, int limit) {
		
		if (mongoOps.getCollection(collectionName).countDocuments() > 0) {
		
			return mongoOps.find(
					new Query().with(Sort.by(sort, key)).limit(limit), 
					BinanceKline.class, 
					collectionName);
			
		}
		
		return Collections.<BinanceKline>emptyList();
		
	}
	
	public BinanceKline getDocumentWithOpenTime(String collectionName, Long openTime) {
		
		return mongoOps.findOne(
				new Query().addCriteria(Criteria.where("openTime").is(openTime)), 
				BinanceKline.class, 
				collectionName);
		
	}
	
	public void insertKlineList(List<BinanceKline> klineList, String collectionName, Long startTime) {

		if (klineList != null && !klineList.isEmpty() && klineList.size() > 0) {
			
			if (this.deleteBeforeInsert) {

				DeleteResult deleteResult = this.mongoOps.remove(new Query(Criteria.where("openTime").gte(startTime)), collectionName);
	
				Core.logL("\t" + deleteResult.getDeletedCount() + " documents deleted.");
			
			}
			
			Collection<BinanceKline> insertedObjects = this.mongoOps.insert(klineList, collectionName);
			
			if (insertedObjects.isEmpty())
				
				Core.logL("\tEmpty array of inserted objects for " + klineList.size() + " documents into collection " + collectionName);
			
			else
				
				Core.logL("\t" + insertedObjects.size() + " objects inserted into collection " + collectionName);

			Core.logL("\t\tDocuments count: " + this.mongoOps.getCollection(collectionName).countDocuments());
			
			if (this.loggingOn) {
				
				Long maxOpenTime = this.getDocumentWithMaxValueOf(collectionName, "openTime").getOpenTime();
			
				Core.logL("\t\t\tLatest openTime in DB: " + maxOpenTime + " = " + this.dateFormat.format(maxOpenTime));
				
			}
		}
		
	}
	
	public long getEarliestOpenTime(String symbol, Interval interval, Long fromOpenTime, Long toOpenTime, boolean fromExists) {
		
	    Long inFrom = 0L;
	    Long inTo = 0L;
	    Long outFrom = 0L;
	    Long outTo = 0L;
	    Long middleUtsMs = 0L;
	    boolean inFromExists = false;
	    boolean outFromExists = false;
	    Interval intervalToUse;
	    
	    if (interval.getCode() == "3d") intervalToUse = Interval._1d; else intervalToUse = interval;

	    if ( fromOpenTime == 0L )
	    {
			inFrom = Constants.truncByInterval(Instant.now().getEpochSecond() * 1000, intervalToUse);
			inTo = inFrom;
			inFromExists = true;
	    }
	    else
	    {
	    	inFrom = fromOpenTime;
			inTo = toOpenTime;
			inFromExists = fromExists;
	    }
		
		if (inFromExists) {
			outFrom = Constants.truncByInterval(inFrom - Constants.uts3y, intervalToUse);
			
			if (this.klineExists(symbol, intervalToUse, outFrom))
			{
				outTo = outFrom;
				outFromExists = true;
			}
			else
			{
				outTo = inFrom;
				outFromExists = false;
			}
		}
		else
		{
			if (inFrom == inTo || inTo - inFrom == intervalToUse.getUtsMs())
				return inTo;
			else if (intervalToUse.getCode() == "1M" && Period
					.between((new Date(inFrom)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate().withDayOfMonth(1),
							(new Date(inTo)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate().withDayOfMonth(1))
					.getMonths() == 1)
				return inTo;
            
            middleUtsMs = Constants.truncByInterval((inFrom + inTo) / 2, intervalToUse);
            		
    		if (this.klineExists(symbol, intervalToUse, middleUtsMs))
    		{
    			outFrom = inFrom;
				outTo = middleUtsMs;
				outFromExists = false;
    		}
            else
            {
            	outFrom = middleUtsMs;
    			outTo = inTo;
				outFromExists = false;
            }
		}
		
		return this.getEarliestOpenTime(symbol, intervalToUse, outFrom, outTo, outFromExists);
	}
	
	public void importKlines(String symbol, Interval interval) {
		
		Core.logL("-----------------------\nImport into: " + symbol + "-" + interval.getCode() + "\n-----------------------");
		
		String collectionName = symbol + "-" + interval.getCode();
		
		Long startTime = 0L;
		
		Long documentCount = this.mongoOps.getCollection(collectionName).countDocuments();
		
		Core.logL("\tExisting document count: " + documentCount);
		
		if (documentCount > 0) {

			startTime = this.getDocumentWithMaxValueOf(collectionName, "openTime").getOpenTime();
			
			Core.logL("\tLatest openTime in DB: " + startTime + " = " + this.dateFormat.format(startTime));
			
			startTime += interval.getUtsMs();
			
		}
		else {
			
			Core.logL("\tNo documents exist");
			
			startTime = this.getEarliestOpenTime(symbol, interval, 0L, 0L, false);
			
		}
		
		if (this.firstFetchAllInsertLater) 
			this.insertKlineList(this.fetchKlineList(symbol, interval, startTime), collectionName, startTime);
		else 
			this.fetchAndInsertKlines(symbol, interval, startTime, collectionName);
		
		this.mongoOps.indexOps(collectionName).ensureIndex(new Index().on("openTime", Direction.ASC));
		
		Core.logL("");
		
	}
	
	public Interval getIntervalFromCode(String code) {
		Interval interval = Interval._1M;

		for (Interval i : Interval.values()) { 
		    if (i.getCode().equals(code)) return i; 
		}
		
		return interval;
	}
	
	public List<String> getAllParities() {
		
		List<String> allParities = new ArrayList<String>();
		
		JSONObject jsonObject = new JSONObject((new RestTemplate()).getForObject(this.baseEndpoint + this.exchangeinfoEndpoint, String.class));

		JSONArray symbols = new JSONArray(jsonObject.get("symbols").toString());
		
		for(int i = 0; i < symbols.length(); i++) {

			if (symbols.getJSONObject(i).get("quoteAsset").toString().equals(parityBase) &&
					symbols.getJSONObject(i).get("status").toString().equals("TRADING")) {
				
				if ((new JSONArray(symbols.getJSONObject(i).get("permissions").toString())).toList().contains("SPOT")) {
					
					allParities.add(symbols.getJSONObject(i).get("baseAsset").toString());
					
				}
				
			}
			
		}

		return allParities; 
		
	}
	
	public List<String> getAllIntervals() {
		
		List<String> allIntervals = new ArrayList<String>();
	
		for (Interval i : Interval.values()) { 
			allIntervals.add(i.getCode());
		}
		
		return allIntervals;
		
	}
	
	public void deleteDocumentWithMaxOpenTimeFromAllCollections() {
		
		Long maxOpenTime;
		
		for(Object c : this.mongoOps.getCollectionNames().toArray()) {
			
			maxOpenTime = this.getDocumentWithMaxValueOf(c.toString(), "openTime").getOpenTime();
			
			DeleteResult deleteResult = this.mongoOps.remove(new Query(Criteria.where("openTime").is(maxOpenTime)), c.toString());
			
			Core.logL(c.toString() + ": " + deleteResult.getDeletedCount() + " documents deleted.");
			
		}
		
	}
	
}
