package com.developersertac.BinanceKlineFetcherRsiScanner;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoOperations;

import com.mongodb.client.MongoClient;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Configuration
@NoArgsConstructor
public class Globals implements InitializingBean {

	@Autowired
	private @Getter MongoClient mongoClient;

	@Autowired
	private @Getter MongoOperations mongoOps;
	
	private @Getter DateFormat dateFormat;
    
    @Getter
    @Setter
    public class SpringDataMongoDb {
    	private String uri;
    	private String database; 
    }
    
    @Getter
    @Setter
    public class Binance {
    	private String baseEndpoint;
    	private String klinesEndpoint; 
    	private String timeEndpoint; 
    	private String exchangeinfoEndpoint;
    	private int limit; 
    }
    
    @Getter
    @Setter
    public class Cron {
    	private String expression;
    }
    
    @Getter
    @Setter
    public class Flow {
    	private boolean firstFetchAllInsertLater;
    	private boolean loggingOn;
    	private boolean deleteBeforeInsert;
    }
    
    @Getter
    @Setter
    public class Parity {
    	private String selector;
    	private String base; 
    	private List<String> symbols; 
    	private List<String> intervals; 
    	private List<String> blacklist; 
    	private List<List<String>> info; 
    }
    
    @Getter
    @Setter
    public class Indicator {
    	private boolean calculationOn;
    	private List<List<String>> info; 
    }
	
    @Bean
    @ConfigurationProperties(prefix = "spring.data.mongodb")
    public SpringDataMongoDb springDataMongoDb() {
        return new SpringDataMongoDb();
    }
	
    @Bean
    @ConfigurationProperties(prefix = "binance")
    public Binance binance() {
        return new Binance();
    }
	
    @Bean
    @ConfigurationProperties(prefix = "cron")
    public Cron cron() {
        return new Cron();
    }
	
    @Bean
    @ConfigurationProperties(prefix = "flow")
    public Flow flow() {
        return new Flow();
    }
	
    @Bean
    @ConfigurationProperties(prefix = "parity")
    public Parity parity() {
        return new Parity();
    }
	
    @Bean
    @ConfigurationProperties(prefix = "indicator")
    public Indicator indicator() {
        return new Indicator();
    }

	@Override
	public void afterPropertiesSet() throws Exception {
		
		this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		
		this.dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		
	}
}
