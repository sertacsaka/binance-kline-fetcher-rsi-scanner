package com.developersertac.BinanceKlineFetcherRsiScanner;

import java.math.BigDecimal;

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
@TypeAlias("RSI")
public class Rsi {
	
	public Rsi(Long openTime, BigDecimal averageGain, BigDecimal averageLoss, BigDecimal RSI) {

		this.openTime = openTime;
		this.averageGain = averageGain;
		this.averageLoss = averageLoss;
		this.RSI = RSI;
	}
	
	public Rsi() {

		this.openTime = 0L;
		this.averageGain = new BigDecimal("0");
		this.averageLoss = new BigDecimal("0");
		this.RSI = new BigDecimal("0");
	}

	@Id
	private String id;
	private Long openTime;
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal averageGain;
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal averageLoss;
	@Field(targetType = FieldType.DECIMAL128)
    private BigDecimal RSI;
}