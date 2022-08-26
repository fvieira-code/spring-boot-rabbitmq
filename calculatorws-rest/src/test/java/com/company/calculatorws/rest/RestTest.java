package com.company.calculatorws.rest;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CalculatorWsApplication.class)
@WebAppConfiguration
public class RestTest {

	@Autowired
	private WebApplicationContext ctx;
	
	final String BASE_URL = "http://localhost:8080/";
    private MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(ctx).build();
    }

    @Test
    public void testRest_Sum_10_5() throws Exception {
        this.mockMvc.perform(get("/sum?a=10&b=5"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(15)));
    }
    
    @Test
    public void testRest_Sub_10_5() throws Exception {
        this.mockMvc.perform(get("/sub?a=10&b=5"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(5)));
    }
    
    @Test
    public void testRest_Mul_10_5() throws Exception {
        this.mockMvc.perform(get("/mul?a=10&b=5"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(50)));
    }
    
    @Test
    public void testRest_Div_10_5() throws Exception {
        this.mockMvc.perform(get("/div?a=10&b=5"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(2.0)));
    }
    
    @Test
    public void testRest_Sum_MAXINT_MAXINT() throws Exception {
        this.mockMvc.perform(get("/sum?a=2147483647&b=2147483647"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result").value(Long.parseLong("4294967294")));
    }
    
    @Test
    public void testRest_Div_MAXINT_MAXINT() throws Exception {
        this.mockMvc.perform(get("/div?a=2147483647&b=2147483647"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(1.0)));
    }
    
    @Test
    public void testRest_Sub_MAXINT_MAXINT() throws Exception {
        this.mockMvc.perform(get("/sub?a=2147483647&b=2147483647"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(0)));
    }
    
    @Test
    public void testRest_Mul_MAXINT_MAXINT() throws Exception {
        this.mockMvc.perform(get("/mul?a=2147483647&b=2147483647"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result").value(Long.parseLong("4611686014132420609")));
    }
    
    @Test
    public void testRest_Sum_MAXINT_MININT() throws Exception {
        this.mockMvc.perform(get("/sum?a=2147483647&b=-2147483648"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(-1)));
    }
    
    @Test
    public void testRest_Mul_MAXINT_MININT() throws Exception {
        this.mockMvc.perform(get("/mul?a=2147483647&b=-2147483648"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result").value(Long.parseLong("-4611686016279904256")));
    }    
    
    @Test
    public void testRest_Mul_9o9999_2o8978() throws Exception {
        this.mockMvc.perform(get("/mul?a=9.9999&b=2.8978"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(28.97771022)));
    }
    
    @Test
    public void testRest_Div_10000_3000() throws Exception {
        this.mockMvc.perform(get("/div?a=10000&b=3000"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(3.333334)));
    }
    
    @Test
    public void testRest_Sum_100_N3000() throws Exception {
        this.mockMvc.perform(get("/sum?a=100&b=-3000"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(-2900)));
    }
    
    @Test
    public void testRest_Sub_100_N3000() throws Exception {
        this.mockMvc.perform(get("/sub?a=100&b=-3000"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(3100)));
    }    
    
    @Test
    public void testRest_Sum_0_0() throws Exception {
        this.mockMvc.perform(get("/sum?a=0&b=0"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.result", is(0)));
    }
    
    @Test
    public void testRest_Div_10_0() throws Exception {
        this.mockMvc.perform(get("/div?a=10&b=0"))
        		.andExpect(status().isOk())
        		.andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.statusCode", is(400)))
        		.andExpect(jsonPath("$.message").value("ERROR: Division by 0"));
    }
    
    @Test
    public void testRest_Mul_10_() throws Exception {
        this.mockMvc.perform(get("/mul?a=10")).andExpect(status().isBadRequest());
    }
    
    @Test
    public void testRest_Mod_10_2() throws Exception {
        this.mockMvc.perform(get("/mod?a=10&b=2")).andExpect(status().isNotFound());
    }

}
