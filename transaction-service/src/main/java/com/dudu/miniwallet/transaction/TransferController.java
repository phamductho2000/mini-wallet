package com.dudu.miniwallet.transaction;

import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/transfers")
public class TransferController {
    private final TransferService service;

    public TransferController(TransferService service) { this.service = service; }

    @PostMapping
    public Transfer create(@RequestHeader("Idempotency-Key") String key, @RequestBody Map<String, Object> body) {
        return service.transfer(key, (String) body.get("from"), (String) body.get("to"),
                new BigDecimal(body.get("amount").toString()));
    }
}
