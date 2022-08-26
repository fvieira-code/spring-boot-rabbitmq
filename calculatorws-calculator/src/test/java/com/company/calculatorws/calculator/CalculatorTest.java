package com.company.calculatorws.calculator;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.company.calculatorws.calculator.exception.CalculatorException;
import com.company.calculatorws.calculator.handler.MessageHandler;
import com.company.calculatorws.calculator.handler.MessageHandlerImpl;

public class CalculatorTest {
	
	private MessageHandler messageHandler;

	@Before
	public void setUp() {
		messageHandler = new MessageHandlerImpl();
	}
	
	@Test
	public void testAdd_10_2() {
		assertEquals("12", messageHandler.process("Sum,10,2"));
	}
	
	@Test
	public void testSum_10_2() {
		assertEquals("8", messageHandler.process("Sub,10,2"));
	}
	
	@Test
	public void testMul_10_2() {
		assertEquals("20", messageHandler.process("Mul,10,2"));
	}
	
	@Test
	public void testDiv_10_2() {
		assertEquals("5.000000", messageHandler.process("Div,10,2"));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testDiv_10_0() {
		messageHandler.process("Div,10,0");
	}	
	
	@Test(expected=CalculatorException.class)
	public void testMul_10() {
		messageHandler.process("Mul,10");
	}
	
	@Test(expected=CalculatorException.class)
	public void testMod_10_2() {
		messageHandler.process("Mod,10,2");
	}

}
