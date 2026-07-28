package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.projection.sink.PostgresProjectionSink;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes measured projection lag (event.timestamp() at emission -> the moment the projection
 * sink wrote it to PostgreSQL) so the benchmark report has a real number instead of an assumed
 * one. See benchmark-remediation-guideline.md G3.
 */
@RestController
@RequestMapping("/api/admin/debug")
public class ProjectionLagController {

    private final PostgresProjectionSink projectionSink;

    public ProjectionLagController(PostgresProjectionSink projectionSink) {
        this.projectionSink = projectionSink;
    }

    @GetMapping("/projection-lag")
    public ProjectionLagResponse projectionLag() {
        return new ProjectionLagResponse(projectionSink.getLastProjectionLagMillis());
    }
}
