package com.dudu.miniwallet.transaction;

public class BusinessException extends RuntimeException {
    public BusinessException(String code) { super(code); }
}
