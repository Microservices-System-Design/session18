package com.shopmart.payment.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "payment")
public record PaymentProperties(
        boolean simulateFailure,
        BigDecimal maxAmount
) {
}
