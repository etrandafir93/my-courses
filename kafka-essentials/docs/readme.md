
# Spring Kafka - Essentials

In this tutorial, we’ll build a Spring Boot application that uses Spring Kafka 
to produce and consume messages. 
We’ll start with the basics (producing and consuming plain text messages)
and gradually add more features like defining an Avro schema, writing tests, 
and using Kafka partitions for concurrent processing.

## Demo Application

We'll work on a financial markets application that analyzes price trends for various stocks. 
The project is organized into two main packages:

- [stock.price](../src/main/java/io/github/etr/courses/kafka/stock/price): 
This package detects stock price changes and produces messages to a Kafka topic 
called `stock.price.updates`. 
It also provides a PUT endpoint for sending stock price updates via HTTP, 
which is useful for demos and testing.


- [trend.analysis](../src/main/java/io/github/etr/courses/kafka/trend/analysis): 
This package analyzes stock price updates to detect trends. 
It keeps track of the latest price for each stock in memory.

As we build this application, we'll connect the `trend.analysis` package 
to the stream of stock price updates coming from `stock.price`, using Kafka.


## 1. Producing and Consuming Messages

- [1.1. Producing Kafka Messages](1-1-produce-messages.md)

