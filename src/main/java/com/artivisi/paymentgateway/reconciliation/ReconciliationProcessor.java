package com.artivisi.paymentgateway.reconciliation;

import com.artivisi.paymentgateway.projection.entity.PaymentProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.PaymentProjectionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

@Service
public class ReconciliationProcessor {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationProcessor.class);

    private final PaymentProjectionRepository paymentRepo;

    public ReconciliationProcessor(PaymentProjectionRepository paymentRepo) {
        this.paymentRepo = paymentRepo;
    }

    public record ReconResult(
            String reconciliationId,
            String bankCode,
            LocalDate settlementDate,
            int totalRecords,
            int matchedRecords,
            int discrepancyRecords
    ) {}

    @Transactional
    public ReconResult processEodCsv(String bankCode, LocalDate settlementDate, InputStream csvInputStream) {
        String reconId = UUID.randomUUID().toString();
        int total = 0;
        int matched = 0;
        int discrepancy = 0;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvInputStream, StandardCharsets.UTF_8))) {
            String line;
            boolean isHeader = true;

            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue; // Skip CSV Header (va_number, bank_reference, amount, date)
                }

                String[] columns = line.split(",");
                if (columns.length < 3) continue;

                total++;
                String vaNumber = columns[0].trim();
                String bankReference = columns[1].trim();
                BigDecimal amount = new BigDecimal(columns[2].trim());

                Optional<PaymentProjectionEntity> paymentOpt = paymentRepo.findByBankCodeAndBankReference(bankCode, bankReference);
                if (paymentOpt.isPresent() && paymentOpt.get().getAmount().compareTo(amount) == 0) {
                    matched++;
                } else {
                    discrepancy++;
                    log.warn("EOD Recon Discrepancy Flagged: bank={}, va={}, ref={}, amount={}",
                            bankCode, vaNumber, bankReference, amount);
                }
            }

            log.info("Completed EOD CSV Reconciliation for bank {}: total={}, matched={}, discrepancy={}",
                    bankCode, total, matched, discrepancy);

            return new ReconResult(reconId, bankCode, settlementDate, total, matched, discrepancy);

        } catch (Exception e) {
            log.error("Failed to process EOD CSV reconciliation for bank {}", bankCode, e);
            throw new RuntimeException("EOD CSV Reconciliation processing failed: " + e.getMessage(), e);
        }
    }
}
