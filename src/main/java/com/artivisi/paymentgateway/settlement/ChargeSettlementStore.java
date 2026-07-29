package com.artivisi.paymentgateway.settlement;

import com.artivisi.paymentgateway.web.api.PaymentOutcome;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
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
import java.util.Arrays;
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
 * observe or mutate the same charge until this one commits or rolls back. When that update settles
 * the charge (CLOSED, or an INSTALLMENT reaching its total), the same transaction also marks the
 * paying VA {@code PAID} and every other still-ACTIVE sibling {@code CANCELLED} -- mirroring
 * payment-gateway's (RDBMS) {@code PaymentApplicationService.settleAndCancelSiblings} -- so a
 * cancelled sibling's next inquiry correctly returns NOT_FOUND instead of continuing to answer as
 * if it were still live. Kafka is used only afterward, to broadcast the already-final outcome
 * downstream (PostgresProjectionSink's read model); it is no longer where the correctness decision
 * is made.
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

    private static final String VA_STATUS_ACTIVE = "ACTIVE";
    private static final String VA_STATUS_PAID = "PAID";
    private static final String VA_STATUS_CANCELLED = "CANCELLED";

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

    /** A single VA's own record -- distinct from its charge's status; see {@link #resolveVa}. */
    public record VaRecord(String chargeId, String bankCode, String vaNumber, String status) {}

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

    /** Registers a sibling VA. Synchronous: resolvable via {@link #resolveVa} the instant this returns. */
    public void registerVa(String bankCode, String vaNumber, String chargeId) throws RocksDBException {
        ObjectNode vaNode = objectMapper.createObjectNode();
        vaNode.put("chargeId", chargeId);
        vaNode.put("bankCode", bankCode);
        vaNode.put("vaNumber", vaNumber);
        vaNode.put("status", VA_STATUS_ACTIVE);
        byte[] vaValue = objectMapper.writeValueAsString(vaNode).getBytes(StandardCharsets.UTF_8);

        try (WriteOptions writeOptions = new WriteOptions();
             Transaction txn = db.beginTransaction(writeOptions)) {
            txn.put(vaKey(bankCode, vaNumber), vaValue);
            txn.put(vaKey(null, vaNumber), vaValue);
            // Indexes this VA under its charge so applyPayment can find and cancel siblings without
            // a full store scan -- the value is the bank-specific vaKey, the record to update.
            txn.put(vaByChargeKey(chargeId, bankCode, vaNumber), vaKey(bankCode, vaNumber));
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

    /** Resolves a VA's own record -- its chargeId and its own PAID/CANCELLED/ACTIVE status. */
    public Optional<VaRecord> resolveVa(String bankCode, String vaNumber) throws RocksDBException {
        byte[] bytes = db.get(vaKey(bankCode, vaNumber));
        if (bytes == null && vaNumber != null) {
            bytes = db.get(vaKey(null, vaNumber));
        }
        if (bytes == null) {
            return Optional.empty();
        }
        JsonNode node = objectMapper.readTree(bytes);
        return Optional.of(new VaRecord(
                node.hasNonNull("chargeId") ? node.get("chargeId").asString() : null,
                node.hasNonNull("bankCode") ? node.get("bankCode").asString() : null,
                node.hasNonNull("vaNumber") ? node.get("vaNumber").asString() : null,
                node.hasNonNull("status") ? node.get("status").asString() : null));
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

            // (b) VA resolution -- its chargeId assignment never moves once registered, plain read is
            // sufficient, no lock needed. Its own status (ACTIVE/PAID/CANCELLED) isn't checked here:
            // the charge-level terminal-status check below (c) is what decides accept-vs-double-flag,
            // so a payment against an already-settled sibling still gets a correct, specific outcome.
            byte[] vaBytes = txn.get(readOptions, vaKey(bankCode, vaNumber));
            if (vaBytes == null && vaNumber != null) {
                vaBytes = txn.get(readOptions, vaKey(null, vaNumber));
            }
            if (vaBytes == null) {
                txn.rollback();
                return new PaymentApplicationResult(null, null, PaymentOutcome.REJECTED_INVALID_VA,
                        "No active virtual account found for bankCode " + bankCode + " and vaNumber " + vaNumber);
            }
            JsonNode vaNode = objectMapper.readTree(vaBytes);
            String chargeId = vaNode.hasNonNull("chargeId") ? vaNode.get("chargeId").asString() : null;

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
                settleSiblingVas(txn, readOptions, chargeId, bankCode, vaNumber);
            }
            txn.put(chargeKeyBytes, objectMapper.writeValueAsString(chargeNode).getBytes(StandardCharsets.UTF_8));
            recordIdempotency(txn, idempotencyKey, eventId, chargeId, amount, false);
            txn.commit();

            return new PaymentApplicationResult(eventId, chargeId, PaymentOutcome.ACCEPTED, "Payment accepted");
        }
    }

    /**
     * Marks the paying VA {@link #VA_STATUS_PAID} and every other still-{@link #VA_STATUS_ACTIVE}
     * sibling {@link #VA_STATUS_CANCELLED}, mirroring payment-gateway's (RDBMS)
     * {@code PaymentApplicationService.settleAndCancelSiblings} -- only ACTIVE siblings are
     * touched, so a sibling already retired for some other reason is left alone. Runs inside the
     * caller's transaction, after the charge row has already been locked via getForUpdate, so no
     * other transaction can be concurrently doing this same walk for this same chargeId.
     */
    private void settleSiblingVas(Transaction txn, ReadOptions readOptions, String chargeId,
                                   String payingBankCode, String payingVaNumber) throws RocksDBException {
        byte[] payingVaKey = vaKey(payingBankCode, payingVaNumber);
        byte[] prefix = vaByChargePrefix(chargeId);
        try (RocksIterator iterator = txn.getIterator(readOptions)) {
            for (iterator.seek(prefix); iterator.isValid() && hasPrefix(iterator.key(), prefix); iterator.next()) {
                byte[] vaKeyBytes = iterator.value();
                byte[] vaRecordBytes = txn.getForUpdate(readOptions, vaKeyBytes, true);
                if (vaRecordBytes == null) {
                    continue;
                }
                ObjectNode vaNode = (ObjectNode) objectMapper.readTree(vaRecordBytes);
                String vaStatus = vaNode.hasNonNull("status") ? vaNode.get("status").asString() : VA_STATUS_ACTIVE;
                if (!VA_STATUS_ACTIVE.equals(vaStatus)) {
                    continue;
                }
                String vaNumber = vaNode.hasNonNull("vaNumber") ? vaNode.get("vaNumber").asString() : null;
                vaNode.put("status", Arrays.equals(vaKeyBytes, payingVaKey) ? VA_STATUS_PAID : VA_STATUS_CANCELLED);
                byte[] updated = objectMapper.writeValueAsString(vaNode).getBytes(StandardCharsets.UTF_8);
                txn.put(vaKeyBytes, updated);
                byte[] aliasKey = vaKey(null, vaNumber);
                if (!Arrays.equals(aliasKey, vaKeyBytes)) {
                    txn.put(aliasKey, updated);
                }
            }
        }
    }

    private static boolean hasPrefix(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) {
                return false;
            }
        }
        return true;
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

    private static byte[] vaByChargeKey(String chargeId, String bankCode, String vaNumber) {
        return (new String(vaByChargePrefix(chargeId), StandardCharsets.UTF_8) + bankCode + "_" + vaNumber)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] vaByChargePrefix(String chargeId) {
        return ("va_by_charge:" + chargeId + ":").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] idempotencyKey(String bankCode, String bankReference) {
        return ("idem:" + bankCode + "_" + bankReference).getBytes(StandardCharsets.UTF_8);
    }
}
