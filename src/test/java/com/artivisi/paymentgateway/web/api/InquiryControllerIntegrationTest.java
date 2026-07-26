package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

class InquiryControllerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    @DisplayName("REAL INTEGRATION: Inquiry with missing bankCode returns HTTP 400 Bad Request")
    void testInquireAccountMissingBankCode_Negative() {
        InquiryController.AccountInquiryRequest request = new InquiryController.AccountInquiryRequest(
                "",
                "8801928371"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<InquiryController.AccountInquiryRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/inquiry", entity, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("REAL INTEGRATION: Inquiry with missing vaNumber returns HTTP 400 Bad Request")
    void testInquireAccountMissingVaNumber_Negative() {
        InquiryController.AccountInquiryRequest request = new InquiryController.AccountInquiryRequest(
                "MAYBANK",
                ""
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<InquiryController.AccountInquiryRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/inquiry", entity, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
