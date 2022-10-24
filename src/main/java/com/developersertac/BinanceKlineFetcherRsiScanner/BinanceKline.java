package com.developersertac.BinanceKlineFetcherRsiScanner;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Document
@Getter
@Setter
@AllArgsConstructor
@TypeAlias("BinanceKline")
public class BinanceKline {
	
	public BinanceKline(String[] field) {

		this.openTime = Long.parseLong(field[0].toString());
		this.open = new BigDecimal(field[1].toString());
		this.high = new BigDecimal(field[2].toString());
		this.low = new BigDecimal(field[3].toString());
		this.close = new BigDecimal(field[4].toString());
		this.volume = new BigDecimal(field[5].toString());
		this.closeTime = Long.parseLong(field[6].toString());
		this.quoteAssetVolume = new BigDecimal(field[7].toString());
		this.numberOfTrades = BigInteger.valueOf(Long.parseLong(field[8].toString()));
		this.takerBuyBaseAssetVolume = new BigDecimal(field[9].toString());
		this.takerBuyQuoteAssetVolume = new BigDecimal(field[10].toString());
		this.ignore = field[11].toString();
	}
	
	public BinanceKline() {

		this.openTime = 0L;
		this.open = new BigDecimal("0");
		this.high = new BigDecimal("0");
		this.low = new BigDecimal("0");
		this.close = new BigDecimal("0");
		this.volume = new BigDecimal("0");
		this.closeTime = 0L;
		this.quoteAssetVolume = new BigDecimal("0");
		this.numberOfTrades = new BigInteger("0");
		this.takerBuyBaseAssetVolume = new BigDecimal("0");
		this.takerBuyQuoteAssetVolume = new BigDecimal("0");
		this.ignore = "";
	}
    
	@Id
	private String id;
	private Long openTime; //1499040000000
	@Field(targetType = FieldType.DECIMAL128)
	private BigDecimal open; //"0.01634790"
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal high; //"0.80000000"
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal low; //"0.01575800"
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal close; //"0.01577100"
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal volume; //"148976.11427815"
    private Long closeTime; //1499644799999
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal quoteAssetVolume; //"2434.19055334"
    private BigInteger numberOfTrades; // 308
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal takerBuyBaseAssetVolume; //"1756.87402397"
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal takerBuyQuoteAssetVolume; //"28.46694368"
    private String ignore; //"17928899.62484339"
}