package com.company.calculatorws.rest.controller;

import java.io.IOException;
import java.math.BigDecimal;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.company.calculatorws.rest.response.CalcResponse;
import com.company.calculatorws.rest.service.CalcService;

@RestController
public class CalcController {

	@Autowired
	private CalcService calcService;

	@RequestMapping("/sum")
    public ResponseEntity<CalcResponse> sum(@RequestParam(value="a") BigDecimal a, @RequestParam(value="b") BigDecimal b) {
		CalcResponse calcResponse = calcService.send("Sum", a, b);
        return new ResponseEntity<CalcResponse>(calcResponse, HttpStatus.OK);
    }
    
    @RequestMapping("/sub")
    public ResponseEntity<CalcResponse> sub(@RequestParam(value="a") BigDecimal a, @RequestParam(value="b") BigDecimal b) {
    	CalcResponse calcResponse = calcService.send("Sub", a, b);
        return new ResponseEntity<CalcResponse>(calcResponse, HttpStatus.OK);
    }
    
    @RequestMapping("/mul")
    public ResponseEntity<CalcResponse> div(@RequestParam(value="a") BigDecimal a, @RequestParam(value="b") BigDecimal b) {
    	CalcResponse calcResponse = calcService.send("Mul", a, b);
        return new ResponseEntity<CalcResponse>(calcResponse, HttpStatus.OK);    	
    }
    
    @RequestMapping("/div")
    public ResponseEntity<CalcResponse> mul(@RequestParam(value="a") BigDecimal a, @RequestParam(value="b") BigDecimal b) {
    	CalcResponse calcResponse = calcService.send("Div", a, b);
        return new ResponseEntity<CalcResponse>(calcResponse, HttpStatus.OK);    	
    }
    
    @ExceptionHandler({IllegalArgumentException.class, NullPointerException.class})
    void handleBadRequests(HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.BAD_REQUEST.value(), "Invalid parameter informed!");
    }
}
