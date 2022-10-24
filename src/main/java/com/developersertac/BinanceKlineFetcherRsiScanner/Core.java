package com.developersertac.BinanceKlineFetcherRsiScanner;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
//import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class Core implements InitializingBean {
	
	@Autowired
	private Globals globals;
	
	private KlineData klineData;
	private RsiData rsiData;

	private static boolean isLoggingOn;
	
	ExecutorService executor;
	CountDownLatch countDownLatch;
	
    public static void logL(String log) {
    	
    	if (Core.isLoggingOn) System.out.println(log);
    	
    }
    
    public static void log(String log) {
    	
    	if (Core.isLoggingOn) System.out.print(log);
    	
    }
	
//	@Scheduled(cron = "${cron.expression}")
    @Bean
	public void DatabaseUpdate() {
		
		String paritySelector = globals.parity().getSelector();

		List<String> paritySymbols = globals.parity().getSymbols();
		List<String> parityIntervals = globals.parity().getIntervals();
		List<String> parityBlacklist = globals.parity().getBlacklist();
		
		List<List<String>> parityInfo = globals.parity().getInfo();

		
		boolean indicatorCalculationOn = globals.indicator().isCalculationOn();
		List<List<String>> indicatorInfo = globals.indicator().getInfo();
		
		List<String> paritySymbolList = null;
		List<String> parityIntervalList = null;
		
		if (paritySelector.equals("1") || paritySelector.equals("2")) {

			System.out.println("\nSystem Time: " + System.currentTimeMillis());
			System.out.println("Server Time: " + klineData.getServerTime(false));
			
			long importDuration = System.currentTimeMillis();
			long threadsDuration = importDuration;
			
			int simultaneouslyThreadsCount = Runtime.getRuntime().availableProcessors();
				
			if (paritySelector.equals("1")) {
				
				paritySymbolList = (paritySymbols.get(0).equals("all")) ? klineData.getAllParities() : paritySymbols;
				parityIntervalList = (parityIntervals.get(0).equals("all")) ? klineData.getAllIntervals() : parityIntervals;
				
				if (indicatorCalculationOn) {
					
					int totalThreadsCount = paritySymbolList.size() * parityIntervalList.size();
					
					executor = Executors.newFixedThreadPool(simultaneouslyThreadsCount);
					countDownLatch = new CountDownLatch(totalThreadsCount);
				
					logL("\nCreated a newFixedThreadPool with " + simultaneouslyThreadsCount + " threads for total " + totalThreadsCount + " tasks\n");
					
				}
				
				for(String symbol : paritySymbolList) {
					
					if (!parityBlacklist.contains(symbol)) {
					
						for(String code : parityIntervalList) {
							
							String s = symbol + globals.parity().getBase();
							Interval i = klineData.getIntervalFromCode(code);
	
							this.klineData.importKlines(s, i);
							
							if (indicatorCalculationOn) {
							
								for(List<String> iInfo : indicatorInfo) {
								
									if (iInfo.get(0).equals("RSI")) {
		
										executor.execute(rsiData.RsiTask(s, i, Integer.valueOf(iInfo.get(1)), countDownLatch));
										
									}
									
								}
								
							}
	
						}
						
					} else {
						
						if (indicatorCalculationOn) {
							
							for(int i = 0; i < parityIntervalList.size(); i++) countDownLatch.countDown();
							
						}
						
					}
				}
				
			}
			
			if (paritySelector.equals("2")) {
	
				if (indicatorCalculationOn) {
					
					int totalThreadsCount = 0;
					
					for(List<String> pInfo : parityInfo) { totalThreadsCount += pInfo.size() - 1; }
					
					executor = Executors.newFixedThreadPool(simultaneouslyThreadsCount);
					countDownLatch = new CountDownLatch(totalThreadsCount);
				
					logL("\nCreated a newFixedThreadPool with " + simultaneouslyThreadsCount + " threads for total " + totalThreadsCount + " tasks\n");
					
				}
				
				for(List<String> pInfo : parityInfo) {
					
					for(int f = 1; f < pInfo.size(); f++) {
						
						String s = pInfo.get(0);
						Interval i = klineData.getIntervalFromCode(pInfo.get(f));
						
						this.klineData.importKlines(s, i);
						
						if (indicatorCalculationOn) {
						
							for(List<String> iInfo : indicatorInfo) {
							
								if (iInfo.get(0).equals("RSI")) {
								
									executor.execute(rsiData.RsiTask(s, i, Integer.valueOf(iInfo.get(1)), countDownLatch));
									
								}
								
							}
						
						}
						
					}
				}
				
			}
			
			importDuration = System.currentTimeMillis() - importDuration;
			
			System.out.println("");
			System.out.println("Import completed in " + (importDuration/(1000*60)) + " minutes " + ((importDuration/1000)%60) + " seconds " + (importDuration%1000) + " milliseconds");
			System.out.println("");
			
			if (indicatorCalculationOn) {
			
				try {
					countDownLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				threadsDuration = System.currentTimeMillis() - threadsDuration;
				
				System.out.println("");
				System.out.println("RSI tasks completed in " + (threadsDuration/(1000*60)) + " minutes " + ((threadsDuration/1000)%60) + " seconds " + (threadsDuration%1000) + " milliseconds");
				System.out.println("");
				
				this.executor.shutdown();
				
			}
			
		}
		
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		
		Core.isLoggingOn = globals.flow().isLoggingOn();

		this.klineData = new KlineData(globals);
		this.rsiData = new RsiData(globals);
		
	}
}
