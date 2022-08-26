package com.company.calculatorws.rest.service;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.company.calculatorws.rest.client.RPCClient;
import com.company.calculatorws.rest.exception.RestException;
import com.company.calculatorws.rest.response.CalcResponse;

@ComponentScan
@Service
public class CalcServiceImpl implements CalcService {

	@Autowired
	private RPCClient rpcClient;

	@Override
	public CalcResponse send(String oper, BigDecimal a, BigDecimal b) {
		String message = String.format("%s,%s,%s", oper, a, b);
		
		String response = null;
		try {
			response = rpcClient.call(message);
		}
		catch (Exception e) {
			logger.error("An error has occurred while invoke the call method!", e);
			throw new RestException(HttpStatus.INTERNAL_SERVER_ERROR, "Unable to complete request.");
		}
		
		if ( response.startsWith("ERROR") )
			throw new RestException(HttpStatus.BAD_REQUEST, response);		
		
		return new CalcResponse(response);
	}
	
	private static final Logger logger = LoggerFactory.getLogger(CalcServiceImpl.class);

}
