package com.artivisi.paymentgateway;

import com.artivisi.paymentgateway.web.api.BankCallbackController;
import com.artivisi.paymentgateway.web.api.ChargeController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentGatewayEvtsrcApplicationTests extends AbstractIntegrationTest {

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
