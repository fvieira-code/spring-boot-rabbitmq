package com.company.calculatorws.calculator.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CalculatorServer implements CommandLineRunner {

	@Autowired
	private RPCServer rpcServer;

	@Override
    public void run(String... args) throws Exception {
		logger.info("Starting RPCServer ...");
		rpcServer.start();
    }
	
	private static final Logger logger = LoggerFactory.getLogger(CalculatorServer.class);
}
