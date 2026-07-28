package com.artivisi.paymentgateway.web.api.bsi;

import java.math.BigDecimal;

/**
 * Inbound BSI message (bank -> gateway). One endpoint, dispatched on {@code action}.
 * Field-for-field mirror of the relational gateway's proprietary BSI payload so both systems
 * can be driven by the identical wire format.
 */
public record BsiRequest(
        String action,
        String checksum,
        String nomorPembayaran,
        String nomorInvoice,
        String idTransaksi,
        BigDecimal nilai,
        String tanggalTransaksi,
        String kodeBank,
        String kodeChannel,
        String kodeTerminal
) {
}
