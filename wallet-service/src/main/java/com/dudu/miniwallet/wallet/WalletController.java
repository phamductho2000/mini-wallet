package com.dudu.miniwallet.wallet;

import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/wallets")
public class WalletController {
    private final WalletRepository repo;

    public WalletController(WalletRepository repo) { this.repo = repo; }

    // Seed ví để test: POST /wallets {"id":"A","balance":1000}
    @PostMapping
    public Wallet create(@RequestBody Map<String, Object> body) {
        return repo.save(new Wallet((String) body.get("id"), new BigDecimal(body.get("balance").toString())));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Wallet> get(@PathVariable String id) {
        return repo.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/debit")
    @Transactional
    public ResponseEntity<?> debit(@PathVariable String id, @RequestBody Map<String, Object> body) {
        BigDecimal amount = new BigDecimal(body.get("amount").toString());
        if (repo.findById(id).isEmpty()) return ResponseEntity.status(404).body(Map.of("error", "WALLET_NOT_FOUND"));
        // 0 row updated => không đủ tiền
        if (repo.debit(id, amount) == 0) return ResponseEntity.status(422).body(Map.of("error", "INSUFFICIENT_BALANCE"));
        return ResponseEntity.ok(Map.of("status", "DEBITED"));
    }

    @PostMapping("/{id}/credit")
    @Transactional
    public ResponseEntity<?> credit(@PathVariable String id, @RequestBody Map<String, Object> body) {
        if (repo.credit(id, new BigDecimal(body.get("amount").toString())) == 0)
            return ResponseEntity.status(404).body(Map.of("error", "WALLET_NOT_FOUND"));
        return ResponseEntity.ok(Map.of("status", "CREDITED"));
    }
}
