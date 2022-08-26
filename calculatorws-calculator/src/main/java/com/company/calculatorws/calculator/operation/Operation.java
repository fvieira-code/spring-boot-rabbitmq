package com.company.calculatorws.calculator.operation;

import java.math.BigDecimal;

public interface Operation {

	BigDecimal calculate(BigDecimal a, BigDecimal b);
	
}
