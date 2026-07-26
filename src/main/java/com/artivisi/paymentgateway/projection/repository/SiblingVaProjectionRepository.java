package com.artivisi.paymentgateway.projection.repository;

import com.artivisi.paymentgateway.projection.entity.SiblingVaProjectionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SiblingVaProjectionRepository extends JpaRepository<SiblingVaProjectionEntity, String> {

    List<SiblingVaProjectionEntity> findByChargeId(String chargeId);

    Optional<SiblingVaProjectionEntity> findByBankCodeAndVaNumber(String bankCode, String vaNumber);
}
