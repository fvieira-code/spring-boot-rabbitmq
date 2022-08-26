package com.company.calculatorws.rest.response;

import java.math.BigDecimal;

public class CalcResponse {

	private final BigDecimal result;
	
	public CalcResponse(BigDecimal result) {
		this.result = result;
	}
	
	public CalcResponse(String result) {
		this.result = new BigDecimal(result);
	}

	public BigDecimal getResult() {
		return result;
	}
	
}
