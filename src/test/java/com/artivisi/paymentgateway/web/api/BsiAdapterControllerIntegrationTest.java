package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import com.artivisi.paymentgateway.TestSupport;
import com.artivisi.paymentgateway.config.BankSecretProperties;
import com.artivisi.paymentgateway.web.api.bsi.BsiChecksum;
import com.artivisi.paymentgateway.web.api.bsi.BsiRequest;
import com.artivisi.paymentgateway.web.api.bsi.BsiResponseCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the BSI proprietary adapter end to end (checksum -> RocksDB pre-validation ->
 * Kafka Streams topology) using the same wire format the relational gateway's production
 * adapter implements, so the two systems can be driven by an identical k6 workload.
 */
class BsiAdapterControllerIntegrationTest extends AbstractIntegrationTest {

    private static final String BANK_CODE = "BSI";
    private static final DateTimeFormatter TANGGAL_TRANSAKSI_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private BankSecretProperties bankSecretProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("REAL INTEGRATION: Valid BSI payment callback with correct checksum returns SUCCESS")
    void testBsiPayment_ValidChecksum_Accepted() {
        String vaNumber = "7700192001";
        BigDecimal amount = new BigDecimal("750000.00");
        createChargeAndAwaitHydration(vaNumber, amount);

        String tanggalTransaksi = currentTanggalTransaksi();
        String checksum = BsiChecksum.compute(vaNumber, bsiSharedSecret(), tanggalTransaksi);
        BsiRequest request = new BsiRequest("payment", checksum, vaNumber, null,
                "IDTRX-" + UUID.randomUUID(), amount, tanggalTransaksi, "451", "6011", "TERM-01");

        ResponseEntity<String> response = postBsi(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"responseCode\":\"" + BsiResponseCode.SUCCESS + "\"");
    }

    @Test
    @DisplayName("REAL INTEGRATION: BSI callback with an invalid checksum is rejected, not processed")
    void testBsiPayment_InvalidChecksum_Rejected() {
        String vaNumber = "7700192002";
        BigDecimal amount = new BigDecimal("250000.00");
        createChargeAndAwaitHydration(vaNumber, amount);

        String tanggalTransaksi = currentTanggalTransaksi();
        BsiRequest request = new BsiRequest("payment", "0000000000000000000000000000000000000000", vaNumber, null,
                "IDTRX-" + UUID.randomUUID(), amount, tanggalTransaksi, "451", "6011", "TERM-01");

        ResponseEntity<String> response = postBsi(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"responseCode\":\"" + BsiResponseCode.INVALID_CHECKSUM + "\"");
    }

    @Test
    @DisplayName("REAL INTEGRATION: BSI payment callback against an unregistered VA returns INVALID_ACCOUNT")
    void testBsiPayment_UnknownVa_Rejected() {
        String vaNumber = "NO-SUCH-VA-" + UUID.randomUUID();
        String tanggalTransaksi = currentTanggalTransaksi();
        String checksum = BsiChecksum.compute(vaNumber, bsiSharedSecret(), tanggalTransaksi);
        BsiRequest request = new BsiRequest("payment", checksum, vaNumber, null,
                "IDTRX-" + UUID.randomUUID(), new BigDecimal("100000.00"), tanggalTransaksi, "451", "6011", "TERM-01");

        ResponseEntity<String> response = postBsi(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"responseCode\":\"" + BsiResponseCode.INVALID_ACCOUNT + "\"");
    }

    @Test
    @DisplayName("REAL INTEGRATION: BSI payment callback with a blank nomorPembayaran (past a valid checksum) returns INVALID_REQUEST_FORMAT")
    void testBsiPayment_BlankNomorPembayaran_RejectedInvalidRequest() {
        // BsiRequest carries no Bean Validation annotations (the bank's own wire shape has none),
        // so this exercises PaymentApplicationService's own explicit missing-field rejection
        // (PaymentOutcome.REJECTED_INVALID_REQUEST) end to end, which the generic Maybank-shaped
        // endpoint's @Valid DTO makes otherwise unreachable in an integration test.
        String vaNumber = "";
        String tanggalTransaksi = currentTanggalTransaksi();
        String checksum = BsiChecksum.compute(vaNumber, bsiSharedSecret(), tanggalTransaksi);
        BsiRequest request = new BsiRequest("payment", checksum, vaNumber, null,
                "IDTRX-" + UUID.randomUUID(), new BigDecimal("100000.00"), tanggalTransaksi, "451", "6011", "TERM-01");

        ResponseEntity<String> response = postBsi(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"responseCode\":\"" + BsiResponseCode.INVALID_REQUEST_FORMAT + "\"");
    }

    @Test
    @DisplayName("REAL INTEGRATION: BSI inquiry against a hydrated VA returns SUCCESS with the charge amount")
    void testBsiInquiry_Success() {
        String vaNumber = "7700192003";
        BigDecimal amount = new BigDecimal("999000.00");
        createChargeAndAwaitHydration(vaNumber, amount);

        String tanggalTransaksi = currentTanggalTransaksi();
        String checksum = BsiChecksum.compute(vaNumber, bsiSharedSecret(), tanggalTransaksi);
        BsiRequest request = new BsiRequest("inquiry", checksum, vaNumber, null,
                "IDTRX-" + UUID.randomUUID(), null, tanggalTransaksi, "451", "6011", "TERM-01");

        ResponseEntity<String> response = postBsi(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode node = objectMapper.readTree(response.getBody());
        assertThat(node.get("responseCode").asString()).isEqualTo(BsiResponseCode.SUCCESS);
        assertThat(new BigDecimal(node.get("tagihanEfektif").asString())).isEqualByComparingTo(amount);
    }

    private String bsiSharedSecret() {
        return bankSecretProperties.getSecret(BANK_CODE);
    }

    private String currentTanggalTransaksi() {
        return ZonedDateTime.now(ZoneId.of("Asia/Jakarta")).format(TANGGAL_TRANSAKSI_FORMAT);
    }

    private String createChargeAndAwaitHydration(String vaNumber, BigDecimal totalAmount) {
        CreateChargeRequest chargeRequest = new CreateChargeRequest(
                "CLIENT-BSI-ADAPTER-TEST",
                "CLOSED",
                totalAmount,
                "BSI adapter integration test charge",
                List.of(new SiblingVaRequest(BANK_CODE, vaNumber))
        );

        ResponseEntity<CreateChargeResponse> chargeResponse = restTemplate.postForEntity(
                "/api/v1/charges", chargeRequest, CreateChargeResponse.class);
        assertThat(chargeResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String chargeId = chargeResponse.getBody().chargeId();

        TestSupport.awaitChargeStatus(restTemplate, chargeId, "ACTIVE");
        TestSupport.awaitVaResolvable(restTemplate, BANK_CODE, vaNumber);
        return chargeId;
    }

    private ResponseEntity<String> postBsi(BsiRequest request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BsiRequest> entity = new HttpEntity<>(request, headers);
        return restTemplate.postForEntity("/api/bank/bsi", entity, String.class);
    }
}
