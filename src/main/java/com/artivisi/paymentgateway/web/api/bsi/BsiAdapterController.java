package com.artivisi.paymentgateway.web.api.bsi;

import com.artivisi.paymentgateway.config.BankSecretProperties;
import com.artivisi.paymentgateway.service.InquiryApplicationService;
import com.artivisi.paymentgateway.service.PaymentApplicationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * BSI adapter (proprietary REST/JSON), dedicated to the {@code BSI} logical bank code so this
 * codebase can be benchmarked through the identical production BSI protocol as the relational
 * gateway. One endpoint, dispatched on {@code action}: {@code inquiry} | {@code payment}.
 * {@code reversal} is explicitly out of scope for this adapter and maps to INVALID_ACTION.
 *
 * <p>evtsrc has no {@code EscrowAccount} aggregate (unlike the relational gateway, which resolves
 * one shared secret per escrow); the shared secret is instead resolved from configuration for the
 * single hardcoded logical bank code {@code BSI} (see {@link BankSecretProperties}).
 *
 * <p>Checksum is verified first, before any business logic, per the production protocol. Failures
 * map to BSI response codes carried in the HTTP 200 body (never thrown to the bank) — this mirrors
 * the wire behavior the relational gateway's adapter implements.
 */
@RestController
@RequestMapping("/api/bank/bsi")
public class BsiAdapterController {

    private static final Logger log = LoggerFactory.getLogger(BsiAdapterController.class);
    private static final String BANK_CODE = "BSI";
    /** BSI's terminals send transaction times in Asia/Jakarta, without an offset. */
    private static final ZoneId ZONE = ZoneId.of("Asia/Jakarta");
    private static final DateTimeFormatter BSI_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private final BankSecretProperties bankSecretProperties;
    private final InquiryApplicationService inquiryApplicationService;
    private final PaymentApplicationService paymentApplicationService;

    public BsiAdapterController(BankSecretProperties bankSecretProperties,
                                 InquiryApplicationService inquiryApplicationService,
                                 PaymentApplicationService paymentApplicationService) {
        this.bankSecretProperties = bankSecretProperties;
        this.inquiryApplicationService = inquiryApplicationService;
        this.paymentApplicationService = paymentApplicationService;
    }

    @PostMapping
    public BsiResponse handle(@RequestBody BsiRequest request) {
        try {
            String sharedSecret = bankSecretProperties.getSecret(BANK_CODE);
            String expected = BsiChecksum.compute(request.nomorPembayaran(), sharedSecret, request.tanggalTransaksi());
            if (!BsiChecksum.matches(expected, request.checksum())) {
                return BsiResponse.error(BsiResponseCode.INVALID_CHECKSUM, "Invalid Checksum");
            }

            String action = request.action() == null ? "" : request.action().toLowerCase(Locale.ROOT);
            return switch (action) {
                case "inquiry" -> handleInquiry(request);
                case "payment" -> handlePayment(request);
                default -> BsiResponse.error(BsiResponseCode.INVALID_ACTION, "Action " + action + " tidak dikenal");
            };
        } catch (Exception e) {
            // Never let an unexpected failure escape as Spring's generic (non-BSI-shaped) error
            // body — the bank's parser only understands the BsiResponse wire format.
            log.error("Failed to process BSI callback", e);
            return BsiResponse.error(BsiResponseCode.ERROR, "Internal error processing request");
        }
    }

    private BsiResponse handleInquiry(BsiRequest request) {
        InquiryApplicationService.InquiryResult result =
                inquiryApplicationService.inquireAccount(BANK_CODE, request.nomorPembayaran());

        return switch (result.status()) {
            case "SUCCESS" -> echo(request)
                    .responseCode(BsiResponseCode.SUCCESS)
                    .responseMessage("OK")
                    .nomorPembayaran(request.nomorPembayaran())
                    .nama(result.customerName())
                    .tagihanTotal(result.amount())
                    .tagihanEfektif(result.amount())
                    .tanggalTransaksi(request.tanggalTransaksi())
                    .build();
            case "INVALID_VA", "INVALID_CHARGE" -> BsiResponse.error(BsiResponseCode.INVALID_ACCOUNT, result.message());
            default -> BsiResponse.error(BsiResponseCode.ERROR, result.message());
        };
    }

    private BsiResponse handlePayment(BsiRequest request) throws Exception {
        Instant paymentTimestamp;
        try {
            paymentTimestamp = bankTransactionTime(request.tanggalTransaksi());
        } catch (DateTimeParseException e) {
            return BsiResponse.error(BsiResponseCode.INVALID_REQUEST_FORMAT,
                    "tanggalTransaksi tidak valid: " + request.tanggalTransaksi());
        }

        PaymentApplicationService.PaymentProcessingResult result = paymentApplicationService.processPayment(
                BANK_CODE, request.nomorPembayaran(), request.idTransaksi(), request.nilai(), paymentTimestamp);

        return switch (result.outcome()) {
            case ACCEPTED, DUPLICATE -> echo(request)
                    .responseCode(BsiResponseCode.SUCCESS)
                    .responseMessage(result.message())
                    .nomorPembayaran(request.nomorPembayaran())
                    .referensiPembayaran(result.eventId())
                    .tanggalTransaksi(request.tanggalTransaksi())
                    .build();
            case REJECTED_INVALID_VA -> BsiResponse.error(BsiResponseCode.INVALID_ACCOUNT, result.message());
            case REJECTED_CHARGE_CLOSED, REJECTED_INVALID_AMOUNT ->
                    BsiResponse.error(BsiResponseCode.INVALID_AMOUNT, result.message());
            case REJECTED_INVALID_REQUEST -> BsiResponse.error(BsiResponseCode.INVALID_REQUEST_FORMAT, result.message());
        };
    }

    /** Seeds a response builder with the request-echo fields common to every success reply. */
    private static BsiResponse.Builder echo(BsiRequest request) {
        return BsiResponse.builder()
                .kodeBank(request.kodeBank())
                .kodeChannel(request.kodeChannel())
                .kodeTerminal(request.kodeTerminal())
                .idTransaksi(request.idTransaksi());
    }

    /**
     * The payment's time at the BANK, parsed from {@code tanggalTransaksi} (format
     * {@code yyyyMMddHHmmss}, WIB, no offset — what BSI's terminals send).
     *
     * <p>Deliberately throws rather than falling back to {@code now()}: a silent substitution is
     * exactly how the bank's timestamp came to be discarded in the system this adapter mirrors.
     */
    private static Instant bankTransactionTime(String tanggalTransaksi) {
        if (tanggalTransaksi == null || tanggalTransaksi.isBlank()) {
            throw new DateTimeParseException("tanggalTransaksi is missing", String.valueOf(tanggalTransaksi), 0);
        }
        return LocalDateTime.parse(tanggalTransaksi, BSI_TIMESTAMP_FORMAT).atZone(ZONE).toInstant();
    }
}
