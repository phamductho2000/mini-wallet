package com.dudu.miniwallet.transaction;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import java.math.BigDecimal;
import java.util.Map;

@Service
public class TransferService {
    private final TransferRepository transfers;
    private final OutboxRepository outbox;
    private final WalletClient wallet;
    private final TransactionTemplate tx;

    public TransferService(TransferRepository t, OutboxRepository o, WalletClient w, TransactionTemplate tx) {
        this.transfers = t; this.outbox = o; this.wallet = w; this.tx = tx;
    }

    /** KHÔNG để @Transactional bọc cả đoạn gọi Feign (giữ connection DB trong lúc chờ HTTP là anti-pattern). */
    public Transfer transfer(String key, String from, String to, BigDecimal amount) {
        // 1) Idempotency: key đã có -> trả kết quả cũ, không xử lý lại
        var existing = transfers.findByIdempotencyKey(key);
        if (existing.isPresent()) return existing.get();

        Transfer t;
        try {
            t = transfers.save(new Transfer(key, from, to, amount));   // unique index chặn 2 request song song cùng key
        } catch (DataIntegrityViolationException dup) {
            return transfers.findByIdempotencyKey(key).orElseThrow();
        }

        // 2) Gọi wallet-service (Feign): trừ tiền ví nguồn
        try {
            wallet.debit(from, Map.of("amount", amount));
        } catch (BusinessException be) {
            return finish(t, "FAILED", be.getMessage(), null);
        } catch (Exception e) {
            // timeout / 5xx: KHÔNG biết wallet đã trừ hay chưa -> UNKNOWN, đối soát sau. Không retry mù.
            return finish(t, "UNKNOWN", e.getMessage(), null);
        }

        // 3) Cộng ví đích
        try {
            wallet.credit(to, Map.of("amount", amount));
        } catch (Exception e) {
            // Đã trừ nguồn mà cộng đích lỗi -> cần bù (compensate). Bản mini: đánh dấu để xử lý tay / thực hành Saga.
            return finish(t, "UNKNOWN", "CREDIT_FAILED:" + e.getMessage(), null);
        }

        // 4) Lưu status + outbox trong CÙNG 1 transaction local
        String payload = "{\"transferId\":" + t.getId() + ",\"from\":\"" + from + "\",\"to\":\"" + to
                + "\",\"amount\":" + amount + ",\"idempotencyKey\":\"" + key + "\"}";
        return finish(t, "COMPLETED", null, new OutboxEvent(from, "transfer.completed", payload));
    }

    private Transfer finish(Transfer t, String status, String reason, OutboxEvent event) {
        return tx.execute(s -> {
            t.setStatus(status);
            t.setFailReason(reason);
            Transfer saved = transfers.save(t);
            if (event != null) outbox.save(event);
            return saved;
        });
    }
}
