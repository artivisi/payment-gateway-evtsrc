package com.artivisi.paymentgateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class PaymentGatewayEvtsrcApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentGatewayEvtsrcApplication.class, args);
    }
}
