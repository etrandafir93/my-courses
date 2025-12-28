package com.etrandafir.blog.app;

import java.math.BigDecimal;

import com.etrandafir.blog.samples.MoneyAmount;

public class DummyApp {

    public static void main(String[] args) {
        MoneyAmount money = new MoneyAmount(
            new BigDecimal("100.50"), "USD", "$");

        System.out.println(money);
    }
}
