package com.artivisi.paymentgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Map;

/**
 * Per-bank shared secrets used for proprietary callback checksum verification (e.g. BSI's
 * {@code SHA1(nomorPembayaran + secret + tanggalTransaksi)}).
 *
 * <p>The relational gateway resolves this secret per {@code EscrowAccount} (one secret per
 * bank connection). evtsrc has no such aggregate and does not get one added just for this:
 * a bank code here resolves to a single, configuration-only secret.
 *
 * <p>Fail loud: a bank code with no configured (or blank) secret throws rather than falling
 * back to an empty/default secret. An empty secret makes the checksum trivially forgeable —
 * exactly the bug this replaces (the old {@code seed-db-direct.py} left the secret NULL, so
 * every checksum matched the literal string {@code "null"}).
 */
@Component
@ConfigurationProperties(prefix = "app.bank-secrets")
public class BankSecretProperties {

    private Map<String, String> secrets = Map.of();

    public Map<String, String> getSecrets() {
        return secrets;
    }

    public void setSecrets(Map<String, String> secrets) {
        this.secrets = secrets;
    }

    /**
     * Resolves the shared secret configured for {@code bankCode}. Lookup is case-insensitive
     * against the configured map (YAML keys are conventionally lowercase; bank codes used
     * elsewhere in the system are conventionally uppercase, e.g. {@code "BSI"}).
     *
     * @throws IllegalStateException if bankCode is blank, or no non-blank secret is configured
     */
    public String getSecret(String bankCode) {
        if (bankCode == null || bankCode.isBlank()) {
            throw new IllegalStateException("bank code must not be blank when resolving a shared secret");
        }
        String secret = secrets.get(bankCode.toLowerCase(Locale.ROOT));
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException("no shared secret configured for bank code " + bankCode);
        }
        return secret;
    }
}
