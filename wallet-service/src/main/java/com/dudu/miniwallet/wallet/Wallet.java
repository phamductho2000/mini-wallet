package com.dudu.miniwallet.wallet;

import jakarta.persistence.*;
import java.math.BigDecimal;

@Entity
@Table(name = "wallet")
public class Wallet {
    @Id
    private String id;
    @Column(nullable = false)
    private BigDecimal balance;

    protected Wallet() {}
    public Wallet(String id, BigDecimal balance) { this.id = id; this.balance = balance; }
    public String getId() { return id; }
    public BigDecimal getBalance() { return balance; }
}
