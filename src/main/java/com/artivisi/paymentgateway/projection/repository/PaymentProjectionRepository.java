package com.artivisi.paymentgateway.projection.repository;

import com.artivisi.paymentgateway.projection.entity.PaymentProjectionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentProjectionRepository extends JpaRepository<PaymentProjectionEntity, String> {

    List<PaymentProjectionEntity> findByChargeId(String chargeId);

    Optional<PaymentProjectionEntity> findByBankCodeAndBankReference(String bankCode, String bankReference);
}
