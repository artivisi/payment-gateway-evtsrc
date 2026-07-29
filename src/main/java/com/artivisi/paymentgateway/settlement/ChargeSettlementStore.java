package com.artivisi.paymentgateway.settlement;

import com.artivisi.paymentgateway.web.api.PaymentOutcome;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * The single local source of truth for charge/VA/payment state, directly owning a RocksDB
 * {@link TransactionDB} -- not a Kafka Streams-managed state store.
 *
 * This replaces the earlier design where the request thread did a read-only interactive query
 * against a Kafka-Streams-owned RocksDB store, then published an event that a separate
 * StreamThread applied asynchronously later. That split left a race window: the request thread's
 * optimistic accept and the topology's authoritative decision could disagree, and by the time the
 * topology caught it, the bank had already been told "ACCEPTED" over HTTP (see
 * benchmark-remediation-guideline.md's Sixth/Seventh gap for the incident this fixes).
 *
 * {@link #applyPayment} performs the VA lookup, charge terminal-status check, and the balance
 * update as one atomic RocksDB transaction, using {@code getForUpdate} for the same row-lock
 * semantics {@code SELECT FOR UPDATE} gives a relational transaction -- no other transaction can
 * observe or mutate the same charge until this one commits or rolls back. Kafka is used only
 * afterward, to broadcast the already-final outcome downstream (PostgresProjectionSink's read
 * model); it is no longer where the correctness decision is made.
 *
 * Correct at horizontal scale-out only if the deployment routes requests for a given charge (and
 * all its sibling VAs) to the instance that owns this local store -- e.g. by keying on chargeId the
 * same way a Kafka partition would, and using Kafka Streams' {@code queryMetadataForKey()}-style
 * routing if this is ever split across instances. This starter build is single-instance, so that
 * constraint doesn't bite yet, but it's the trade-off this design makes for a synchronous,
 * zero-Kafka-round-trip write path.
 */
@Component
public class ChargeSettlementStore implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ChargeSettlementStore.class);

    private static final String CHARGE_STATUS_PAID = "PAID";
    private static final String CHARGE_STATUS_CANCELLED = "CANCELLED";
    private static final String CHARGE_STATUS_ACTIVE = "ACTIVE";
    private static final String CHARGE_TYPE_OPEN = "OPEN";

    private final ObjectMapper objectMapper;
    private final TransactionDB db;
    private final Options options;
    private final TransactionDBOptions transactionDBOptions;

    public ChargeSettlementStore(ObjectMapper objectMapper,
                                  @Value("${app.settlement-store.dir:./target/settlement-store}") String storeDir)
            throws RocksDBException, IOException {
        this.objectMapper = objectMapper;
        RocksDB.loadLibrary();
        Files.createDirectories(Path.of(storeDir));
        this.options = new Options().setCreateIfMissing(true);
        this.transactionDBOptions = new TransactionDBOptions();
        this.db = TransactionDB.open(options, transactionDBOptions, storeDir);
        log.info("ChargeSettlementStore opened at {}", storeDir);
    }

    @Override
    public void destroy() {
        db.close();
        transactionDBOptions.close();
        options.close();
    }

    public record PaymentApplicationResult(
            String eventId,
            String chargeId,
            PaymentOutcome outcome,
            String message
    ) {}

    /** Registers a newly-created charge. Synchronous: readable via {@link #getChargeJson} the instant this returns. */
    public void registerCharge(String chargeId, String clientId, String chargeType,
                                BigDecimal totalAmount, String description, Instant timestamp) throws RocksDBException {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("chargeId", chargeId);
        node.put("clientId", clientId);
        node.put("chargeType", chargeType);
        node.put("totalAmount", totalAmount.toPlainString());
        node.put("cumulativePaid", BigDecimal.ZERO.toPlainString());
        node.put("status", CHARGE_STATUS_ACTIVE);
        if (description != null) {
            node.put("description", description);
        }
        node.put("timestamp", timestamp.toString());
        try (WriteOptions writeOptions = new WriteOptions();
             Transaction txn = db.beginTransaction(writeOptions)) {
            txn.put(chargeKey(chargeId), objectMapper.writeValueAsString(node).getBytes(StandardCharsets.UTF_8));
            txn.commit();
        }
    }

    /** Registers a sibling VA. Synchronous: resolvable via {@link #resolveChargeId} the instant this returns. */
    public void registerVa(String bankCode, String vaNumber, String chargeId) throws RocksDBException {
        try (WriteOptions writeOptions = new WriteOptions();
             Transaction txn = db.beginTransaction(writeOptions)) {
            txn.put(vaKey(bankCode, vaNumber), chargeId.getBytes(StandardCharsets.UTF_8));
            txn.put(vaKey(null, vaNumber), chargeId.getBytes(StandardCharsets.UTF_8));
            txn.commit();
        }
    }

    public void cancelCharge(String chargeId) throws RocksDBException {
        try (WriteOptions writeOptions = new WriteOptions();
             ReadOptions readOptions = new ReadOptions();
             Transaction txn = db.beginTransaction(writeOptions)) {
            byte[] key = chargeKey(chargeId);
            byte[] existing = txn.getForUpdate(readOptions, key, true);
            if (existing == null) {
                txn.rollback();
                return;
            }
            ObjectNode node = (ObjectNode) objectMapper.readTree(existing);
            node.put("status", CHARGE_STATUS_CANCELLED);
            txn.put(key, objectMapper.writeValueAsString(node).getBytes(StandardCharsets.UTF_8));
            txn.commit();
        }
    }

    public Optional<String> getChargeJson(String chargeId) throws RocksDBException {
        byte[] bytes = db.get(chargeKey(chargeId));
        return Optional.ofNullable(bytes).map(b -> new String(b, StandardCharsets.UTF_8));
    }

    public Optional<String> resolveChargeId(String bankCode, String vaNumber) throws RocksDBException {
        byte[] bytes = db.get(vaKey(bankCode, vaNumber));
        if (bytes == null && vaNumber != null) {
            bytes = db.get(vaKey(null, vaNumber));
        }
        return Optional.ofNullable(bytes).map(b -> new String(b, StandardCharsets.UTF_8));
    }

    /** Approximate key count across the whole store; used only for the "is this a fresh store" migration check. */
    public long approximateKeyCount() throws RocksDBException {
        return db.getLongProperty("rocksdb.estimate-num-keys");
    }

    /**
     * Atomically resolves the VA, checks the charge's terminal status, and applies the payment --
     * all within one RocksDB transaction. Returns the FINAL, authoritative outcome; there is no
     * later re-decision, so whatever this returns is exactly what the bank should be told.
     */
    public PaymentApplicationResult applyPayment(String bankCode, String vaNumber, String bankReference,
                                                  BigDecimal amount, Instant paymentTimestamp) throws Exception {
        byte[] idempotencyKey = idempotencyKey(bankCode, bankReference);
        try (WriteOptions writeOptions = new WriteOptions();
             ReadOptions readOptions = new ReadOptions();
             Transaction txn = db.beginTransaction(writeOptions)) {

            // (a) Idempotency -- getForUpdate so a concurrent retry of the SAME bankReference is
            // serialized against this one rather than racing it.
            byte[] existingIdem = txn.getForUpdate(readOptions, idempotencyKey, true);
            if (existingIdem != null) {
                JsonNode recorded = objectMapper.readTree(existingIdem);
                txn.rollback();
                return new PaymentApplicationResult(
                        recorded.hasNonNull("eventId") ? recorded.get("eventId").asString() : null,
                        recorded.hasNonNull("chargeId") ? recorded.get("chargeId").asString() : null,
                        PaymentOutcome.DUPLICATE,
                        "bankReference already processed for bankCode " + bankCode);
            }

            // (b) VA resolution -- immutable once registered, plain read is sufficient, no lock needed.
            byte[] vaBytes = txn.get(readOptions, vaKey(bankCode, vaNumber));
            if (vaBytes == null && vaNumber != null) {
                vaBytes = txn.get(readOptions, vaKey(null, vaNumber));
            }
            if (vaBytes == null) {
                txn.rollback();
                return new PaymentApplicationResult(null, null, PaymentOutcome.REJECTED_INVALID_VA,
                        "No active virtual account found for bankCode " + bankCode + " and vaNumber " + vaNumber);
            }
            String chargeId = new String(vaBytes, StandardCharsets.UTF_8);

            // (c) Charge terminal-status check + (d) apply -- getForUpdate is the row lock: no other
            // transaction touching this SAME chargeId can proceed until this one commits/rolls back.
            byte[] chargeKeyBytes = chargeKey(chargeId);
            byte[] chargeBytes = txn.getForUpdate(readOptions, chargeKeyBytes, true);
            if (chargeBytes == null) {
                txn.rollback();
                return new PaymentApplicationResult(null, chargeId, PaymentOutcome.REJECTED_INVALID_VA,
                        "Charge record not found for chargeId " + chargeId);
            }

            ObjectNode chargeNode = (ObjectNode) objectMapper.readTree(chargeBytes);
            String status = chargeNode.hasNonNull("status") ? chargeNode.get("status").asString() : null;
            String eventId = UUID.randomUUID().toString();

            if (CHARGE_STATUS_PAID.equals(status) || CHARGE_STATUS_CANCELLED.equals(status)) {
                // Never silently absorbed: recorded as a flagged double settlement, atomically, in
                // the SAME transaction that discovered it -- there is no later async re-check that
                // could disagree, because there's only ever one decision made, right here.
                recordIdempotency(txn, idempotencyKey, eventId, chargeId, amount, true);
                txn.commit();
                return new PaymentApplicationResult(eventId, chargeId, PaymentOutcome.REJECTED_CHARGE_CLOSED,
                        "Charge " + chargeId + " is already " + status + "; payment flagged as a possible double settlement");
            }

            BigDecimal totalAmount = chargeNode.hasNonNull("totalAmount")
                    ? new BigDecimal(chargeNode.get("totalAmount").asString())
                    : BigDecimal.ZERO;
            BigDecimal cumulativePaid = chargeNode.hasNonNull("cumulativePaid")
                    ? new BigDecimal(chargeNode.get("cumulativePaid").asString())
                    : BigDecimal.ZERO;
            String chargeType = chargeNode.hasNonNull("chargeType") ? chargeNode.get("chargeType").asString() : null;

            BigDecimal newCumulativePaid = cumulativePaid.add(amount);
            chargeNode.put("cumulativePaid", newCumulativePaid.toPlainString());
            // OPEN never reaches a terminal state -- a standing/always-active account (e.g. a
            // donation VA); see payment-gateway/CLAUDE.md's charge lifecycle section.
            if (!CHARGE_TYPE_OPEN.equals(chargeType) && newCumulativePaid.compareTo(totalAmount) >= 0) {
                chargeNode.put("status", CHARGE_STATUS_PAID);
            }
            txn.put(chargeKeyBytes, objectMapper.writeValueAsString(chargeNode).getBytes(StandardCharsets.UTF_8));
            recordIdempotency(txn, idempotencyKey, eventId, chargeId, amount, false);
            txn.commit();

            return new PaymentApplicationResult(eventId, chargeId, PaymentOutcome.ACCEPTED, "Payment accepted");
        }
    }

    private void recordIdempotency(Transaction txn, byte[] idempotencyKey, String eventId, String chargeId,
                                    BigDecimal amount, boolean doubleSettlement) throws RocksDBException {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("eventId", eventId);
        node.put("chargeId", chargeId);
        node.put("amount", amount.toPlainString());
        node.put("doubleSettlement", doubleSettlement);
        txn.put(idempotencyKey, objectMapper.writeValueAsString(node).getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] chargeKey(String chargeId) {
        return ("charge:" + chargeId).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] vaKey(String bankCode, String vaNumber) {
        String key = bankCode != null ? "va:" + bankCode + "_" + vaNumber : "va:" + vaNumber;
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] idempotencyKey(String bankCode, String bankReference) {
        return ("idem:" + bankCode + "_" + bankReference).getBytes(StandardCharsets.UTF_8);
    }
}
