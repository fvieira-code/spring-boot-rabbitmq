package com.company.calculatorws.rest.service;

import java.math.BigDecimal;

import com.company.calculatorws.rest.response.CalcResponse;

public interface CalcService {

	CalcResponse send(String oper, BigDecimal a, BigDecimal b);
	
}
