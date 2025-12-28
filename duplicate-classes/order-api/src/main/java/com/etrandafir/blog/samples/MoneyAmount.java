package com.etrandafir.blog.samples;

import java.math.BigDecimal;

// order-api
public record MoneyAmount(
    BigDecimal amount,
    String currency,
    String currencySymbol
) {
}
