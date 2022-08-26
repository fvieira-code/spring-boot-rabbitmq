package com.company.calculatorws.calculator.operation;

import java.math.BigDecimal;

public class Sum implements Operation {

	public BigDecimal calculate(BigDecimal a, BigDecimal b) {
		return a.add(b);
	}

}
