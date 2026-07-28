package com.artivisi.paymentgateway.web.api.bsi;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

/**
 * BSI proprietary checksum: {@code SHA1(nomorPembayaran + sharedSecret + tanggalTransaksi)}, hex.
 * The shared secret is resolved from {@code app.bank-secrets} (see
 * {@link com.artivisi.paymentgateway.config.BankSecretProperties}). SHA-1 is mandated by the bank
 * protocol; not used anywhere else. Mirrors the relational gateway's adapter exactly.
 */
public final class BsiChecksum {

    private BsiChecksum() {
    }

    public static String compute(String nomorPembayaran, String sharedKey, String tanggalTransaksi) {
        String raw = nullToEmpty(nomorPembayaran) + sharedKey + nullToEmpty(tanggalTransaksi);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            return HexFormat.of().formatHex(digest.digest(raw.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 unavailable", e);
        }
    }

    public static boolean matches(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        // Normalize to lowercase before constant-time compare: bank implementations (including BSI)
        // commonly output uppercase hex; our compute() outputs lowercase via HexFormat.
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                actual.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8));
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
