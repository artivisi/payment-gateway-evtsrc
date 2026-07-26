package com.artivisi.paymentgateway.projection.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
    name = "sibling_va_projection",
    uniqueConstraints = @UniqueConstraint(name = "uq_bank_va", columnNames = {"bank_code", "va_number"})
)
public class SiblingVaProjectionEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "charge_id", nullable = false)
    private UUID chargeId;

    @Column(name = "bank_code", nullable = false, length = 32)
    private String bankCode;

    @Column(name = "va_number", nullable = false, length = 64)
    private String vaNumber;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public SiblingVaProjectionEntity() {}

    public SiblingVaProjectionEntity(UUID id, UUID chargeId, String bankCode, String vaNumber, String status, Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.chargeId = chargeId;
        this.bankCode = bankCode;
        this.vaNumber = vaNumber;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getChargeId() { return chargeId; }
    public void setChargeId(UUID chargeId) { this.chargeId = chargeId; }

    public String getBankCode() { return bankCode; }
    public void setBankCode(String bankCode) { this.bankCode = bankCode; }

    public String getVaNumber() { return vaNumber; }
    public void setVaNumber(String vaNumber) { this.vaNumber = vaNumber; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
