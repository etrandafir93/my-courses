package com.etrandafir.blog.samples;

import java.math.BigDecimal;

// payment-api
public record MoneyAmount(
    BigDecimal amount,
    String currency
) {
}
