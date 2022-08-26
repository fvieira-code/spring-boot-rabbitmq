# CALCULATOR WS
---
**CalculatorWS** is a REST web service implemented in Java using the framework Spring Boot. 
It uses RabbitMQ for the communications between two modules that exists in the WS, **Rest** and **Calculator**. The WS supports the following math operations:  
- Addition
- Subtraction
- Multiplication
- Division

## Start environment

### Rabbitmq docker container
```
docker run --rm --hostname rabbit --name rabbit -p 5672:5672 rabbitmq:3.7.3-alpine
```

### Build project
```
mvn clean package -DskipTests
```

### calculatorws-rest
```
java -jar calculatorws-rest/target/calculatorws-rest-0.0.1.jar
```

### calculatorws-calculator
```
java -jar calculatorws-calculator/target/calculatorws-calculator-0.0.1.jar
```

### Run test-cases
```
mvn test
```

## API's
---

### Addition (sum)
```
curl -i "http://localhost:8080/sum?a={num1}&b={num2}"
```
### Example
```
curl -i "http://localhost:8080/sum?a=10&b=5"
...
HTTP/1.1 200 OK Content-Type: application/json
{"result":15}
```
---

### Subtraction (sub)
```
curl -i "http://localhost:8080/sub?a={num1}&b={num2}"
```
#### Example
```
curl -i "http://localhost:8080/sub?a=10&b=5"
...
HTTP/1.1 200 OK Content-Type: application/json
{"result":5}
```
---

### Multiplication (mul)
```
curl -i "http://localhost:8080/mul?a={num1}&b={num2}"
```
#### Example
```
curl -i "http://localhost:8080/mul?a=10&b=5"
...
HTTP/1.1 200 OK Content-Type: application/json
{"result":50}
```
---

### Division (div)
```
curl -i "http://localhost:8080/divb?a={num1}&b={num2}"
```
#### Example
```
curl -i "http://localhost:8080/div?a=10&b=5"
...
HTTP/1.1 200 OK Content-Type: application/json
{"result":2}
```