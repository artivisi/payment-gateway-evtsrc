package com.artivisi.paymentgateway.web.api.bsi;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;

/**
 * Outbound BSI message (gateway -> bank). Field set + serialization mirror the relational
 * gateway's adapter exactly: {@code NON_EMPTY} omits null/empty fields (echo fields, and fields
 * not applicable to the action/response) exactly as the production adapter does.
 *
 * <p>No Lombok dependency in this module, so the builder is hand-written rather than
 * {@code @Builder}-generated; the field set, order, and behavior are otherwise identical.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public final class BsiResponse {

    private final String responseCode;
    private final String responseMessage;
    private final String action;
    private final String nomorPembayaran;
    private final String nomorInvoice;
    private final String jenisAkun;
    private final String referensiPembayaran;
    private final String referensiReversal;
    private final BigDecimal tagihanTotal;
    private final BigDecimal tagihanEfektif;
    private final BigDecimal akumulasiPembayaran;
    private final String nama;
    private final String keterangan;
    private final String kodeChannel;
    private final String kodeBank;
    private final String kodeTerminal;
    private final String idTransaksi;
    private final String tanggalTransaksi;
    private final String tanggalTransaksiAsal;

    private BsiResponse(Builder builder) {
        this.responseCode = builder.responseCode;
        this.responseMessage = builder.responseMessage;
        this.action = builder.action;
        this.nomorPembayaran = builder.nomorPembayaran;
        this.nomorInvoice = builder.nomorInvoice;
        this.jenisAkun = builder.jenisAkun;
        this.referensiPembayaran = builder.referensiPembayaran;
        this.referensiReversal = builder.referensiReversal;
        this.tagihanTotal = builder.tagihanTotal;
        this.tagihanEfektif = builder.tagihanEfektif;
        this.akumulasiPembayaran = builder.akumulasiPembayaran;
        this.nama = builder.nama;
        this.keterangan = builder.keterangan;
        this.kodeChannel = builder.kodeChannel;
        this.kodeBank = builder.kodeBank;
        this.kodeTerminal = builder.kodeTerminal;
        this.idTransaksi = builder.idTransaksi;
        this.tanggalTransaksi = builder.tanggalTransaksi;
        this.tanggalTransaksiAsal = builder.tanggalTransaksiAsal;
    }

    public String getResponseCode() {
        return responseCode;
    }

    public String getResponseMessage() {
        return responseMessage;
    }

    public String getAction() {
        return action;
    }

    public String getNomorPembayaran() {
        return nomorPembayaran;
    }

    public String getNomorInvoice() {
        return nomorInvoice;
    }

    public String getJenisAkun() {
        return jenisAkun;
    }

    public String getReferensiPembayaran() {
        return referensiPembayaran;
    }

    public String getReferensiReversal() {
        return referensiReversal;
    }

    public BigDecimal getTagihanTotal() {
        return tagihanTotal;
    }

    public BigDecimal getTagihanEfektif() {
        return tagihanEfektif;
    }

    public BigDecimal getAkumulasiPembayaran() {
        return akumulasiPembayaran;
    }

    public String getNama() {
        return nama;
    }

    public String getKeterangan() {
        return keterangan;
    }

    public String getKodeChannel() {
        return kodeChannel;
    }

    public String getKodeBank() {
        return kodeBank;
    }

    public String getKodeTerminal() {
        return kodeTerminal;
    }

    public String getIdTransaksi() {
        return idTransaksi;
    }

    public String getTanggalTransaksi() {
        return tanggalTransaksi;
    }

    public String getTanggalTransaksiAsal() {
        return tanggalTransaksiAsal;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Error reply carries only the code + message (everything else omitted via NON_EMPTY). */
    public static BsiResponse error(String responseCode, String responseMessage) {
        return BsiResponse.builder().responseCode(responseCode).responseMessage(responseMessage).build();
    }

    public static final class Builder {
        private String responseCode;
        private String responseMessage;
        private String action;
        private String nomorPembayaran;
        private String nomorInvoice;
        private String jenisAkun;
        private String referensiPembayaran;
        private String referensiReversal;
        private BigDecimal tagihanTotal;
        private BigDecimal tagihanEfektif;
        private BigDecimal akumulasiPembayaran;
        private String nama;
        private String keterangan;
        private String kodeChannel;
        private String kodeBank;
        private String kodeTerminal;
        private String idTransaksi;
        private String tanggalTransaksi;
        private String tanggalTransaksiAsal;

        private Builder() {
        }

        public Builder responseCode(String responseCode) {
            this.responseCode = responseCode;
            return this;
        }

        public Builder responseMessage(String responseMessage) {
            this.responseMessage = responseMessage;
            return this;
        }

        public Builder action(String action) {
            this.action = action;
            return this;
        }

        public Builder nomorPembayaran(String nomorPembayaran) {
            this.nomorPembayaran = nomorPembayaran;
            return this;
        }

        public Builder nomorInvoice(String nomorInvoice) {
            this.nomorInvoice = nomorInvoice;
            return this;
        }

        public Builder jenisAkun(String jenisAkun) {
            this.jenisAkun = jenisAkun;
            return this;
        }

        public Builder referensiPembayaran(String referensiPembayaran) {
            this.referensiPembayaran = referensiPembayaran;
            return this;
        }

        public Builder referensiReversal(String referensiReversal) {
            this.referensiReversal = referensiReversal;
            return this;
        }

        public Builder tagihanTotal(BigDecimal tagihanTotal) {
            this.tagihanTotal = tagihanTotal;
            return this;
        }

        public Builder tagihanEfektif(BigDecimal tagihanEfektif) {
            this.tagihanEfektif = tagihanEfektif;
            return this;
        }

        public Builder akumulasiPembayaran(BigDecimal akumulasiPembayaran) {
            this.akumulasiPembayaran = akumulasiPembayaran;
            return this;
        }

        public Builder nama(String nama) {
            this.nama = nama;
            return this;
        }

        public Builder keterangan(String keterangan) {
            this.keterangan = keterangan;
            return this;
        }

        public Builder kodeChannel(String kodeChannel) {
            this.kodeChannel = kodeChannel;
            return this;
        }

        public Builder kodeBank(String kodeBank) {
            this.kodeBank = kodeBank;
            return this;
        }

        public Builder kodeTerminal(String kodeTerminal) {
            this.kodeTerminal = kodeTerminal;
            return this;
        }

        public Builder idTransaksi(String idTransaksi) {
            this.idTransaksi = idTransaksi;
            return this;
        }

        public Builder tanggalTransaksi(String tanggalTransaksi) {
            this.tanggalTransaksi = tanggalTransaksi;
            return this;
        }

        public Builder tanggalTransaksiAsal(String tanggalTransaksiAsal) {
            this.tanggalTransaksiAsal = tanggalTransaksiAsal;
            return this;
        }

        public BsiResponse build() {
            return new BsiResponse(this);
        }
    }
}
