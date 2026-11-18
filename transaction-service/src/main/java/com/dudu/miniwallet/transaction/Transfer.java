package com.dudu.miniwallet.transaction;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "transfer", uniqueConstraints = @UniqueConstraint(columnNames = "idempotencyKey"))
public class Transfer {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false)
    private String idempotencyKey;
    private String fromWallet;
    private String toWallet;
    private BigDecimal amount;
    // PENDING -> DEBITED -> COMPLETED | FAILED | UNKNOWN (timeout, cần đối soát)
    private String status;
    private String failReason;
    private Instant createdAt = Instant.now();

    protected Transfer() {}
    public Transfer(String key, String from, String to, BigDecimal amount) {
        this.idempotencyKey = key; this.fromWallet = from; this.toWallet = to; this.amount = amount; this.status = "PENDING";
    }
    public Long getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getFromWallet() { return fromWallet; }
    public String getToWallet() { return toWallet; }
    public BigDecimal getAmount() { return amount; }
    public String getStatus() { return status; }
    public String getFailReason() { return failReason; }
    public void setStatus(String s) { this.status = s; }
    public void setFailReason(String r) { this.failReason = r; }
}
