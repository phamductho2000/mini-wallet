package com.dudu.miniwallet.transaction;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@FeignClient(name = "wallet-service", url = "${wallet-service.url}", configuration = FeignConfig.class)
public interface WalletClient {

    @PostMapping("/wallets/{id}/debit")
    Map<String, Object> debit(@PathVariable("id") String id, @RequestBody Map<String, Object> body);

    @PostMapping("/wallets/{id}/credit")
    Map<String, Object> credit(@PathVariable("id") String id, @RequestBody Map<String, Object> body);
}
