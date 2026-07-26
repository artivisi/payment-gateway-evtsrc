package com.artivisi.paymentgateway.projection.repository;

import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ChargeProjectionRepository extends JpaRepository<ChargeProjectionEntity, String> {

    List<ChargeProjectionEntity> findByClientIdAndStatus(String clientId, String status);

    List<ChargeProjectionEntity> findByClientId(String clientId);
}
