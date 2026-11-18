package com.dudu.miniwallet.wallet;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.math.BigDecimal;

public interface WalletRepository extends JpaRepository<Wallet, String> {

    // Atomic: không read-modify-write => không race condition, không cần lock.
    @Modifying
    @Query("update Wallet w set w.balance = w.balance - :amount where w.id = :id and w.balance >= :amount")
    int debit(@Param("id") String id, @Param("amount") BigDecimal amount);

    @Modifying
    @Query("update Wallet w set w.balance = w.balance + :amount where w.id = :id")
    int credit(@Param("id") String id, @Param("amount") BigDecimal amount);
}
