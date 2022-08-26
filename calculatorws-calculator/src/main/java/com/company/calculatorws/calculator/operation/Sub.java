package com.company.calculatorws.calculator.operation;

import java.math.BigDecimal;

public class Sub implements Operation {

	public BigDecimal calculate(BigDecimal a, BigDecimal b) {
		return a.subtract(b);
	}

}
