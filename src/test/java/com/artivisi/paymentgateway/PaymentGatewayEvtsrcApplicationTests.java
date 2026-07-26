package com.artivisi.paymentgateway;

import com.artivisi.paymentgateway.web.api.BankCallbackController;
import com.artivisi.paymentgateway.web.api.ChargeController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
    "spring.kafka.bootstrap-servers=localhost:9092",
    "spring.datasource.url=jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.flyway.enabled=false",
    "spring.kafka.streams.auto-startup=false"
})
class PaymentGatewayEvtsrcApplicationTests {

    @Autowired(required = false)
    private BankCallbackController bankCallbackController;

    @Autowired(required = false)
    private ChargeController chargeController;

    @Test
    void contextLoads() {
        assertThat(bankCallbackController).isNotNull();
        assertThat(chargeController).isNotNull();
    }
}
