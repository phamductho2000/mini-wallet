package com.dudu.miniwallet.transaction;

import feign.Response;
import feign.RetryableException;
import feign.Retryer;
import feign.codec.ErrorDecoder;
import org.springframework.context.annotation.Bean;

public class FeignConfig {

    // POST chuyển tiền: KHÔNG retry mù. Muốn retry phải dựa vào idempotency key.
    @Bean
    public Retryer retryer() { return Retryer.NEVER_RETRY; }

    @Bean
    public ErrorDecoder errorDecoder() {
        return (methodKey, response) -> switch (response.status()) {
            case 404 -> new BusinessException("WALLET_NOT_FOUND");
            case 422 -> new BusinessException("INSUFFICIENT_BALANCE");
            // 5xx: lỗi hạ tầng -> trạng thái không chắc chắn, KHÔNG tự retry
            default -> new IllegalStateException("wallet-service error " + response.status() + " at " + methodKey);
        };
    }
}
