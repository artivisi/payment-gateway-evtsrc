package com.artivisi.paymentgateway.projection.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "payment_projection")
public class PaymentProjectionEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "charge_id", nullable = false, length = 64)
    private String chargeId;

    @Column(name = "bank_code", nullable = false, length = 32)
    private String bankCode;

    @Column(name = "va_number", nullable = false, length = 64)
    private String vaNumber;

    @Column(name = "bank_reference", nullable = false, length = 128)
    private String bankReference;

    @Column(name = "amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(name = "payment_timestamp", nullable = false)
    private Instant paymentTimestamp;

    @Column(name = "is_double_settlement", nullable = false)
    private boolean doubleSettlement;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public PaymentProjectionEntity() {}

    public PaymentProjectionEntity(String id, String chargeId, String bankCode, String vaNumber, String bankReference,
                                   BigDecimal amount, Instant paymentTimestamp, boolean doubleSettlement, Instant createdAt) {
        this.id = id;
        this.chargeId = chargeId;
        this.bankCode = bankCode;
        this.vaNumber = vaNumber;
        this.bankReference = bankReference;
        this.amount = amount;
        this.paymentTimestamp = paymentTimestamp;
        this.doubleSettlement = doubleSettlement;
        this.createdAt = createdAt;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getChargeId() { return chargeId; }
    public void setChargeId(String chargeId) { this.chargeId = chargeId; }

    public String getBankCode() { return bankCode; }
    public void setBankCode(String bankCode) { this.bankCode = bankCode; }

    public String getVaNumber() { return vaNumber; }
    public void setVaNumber(String vaNumber) { this.vaNumber = vaNumber; }

    public String getBankReference() { return bankReference; }
    public void setBankReference(String bankReference) { this.bankReference = bankReference; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public Instant getPaymentTimestamp() { return paymentTimestamp; }
    public void setPaymentTimestamp(Instant paymentTimestamp) { this.paymentTimestamp = paymentTimestamp; }

    public boolean isDoubleSettlement() { return doubleSettlement; }
    public void setDoubleSettlement(boolean doubleSettlement) { this.doubleSettlement = doubleSettlement; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
