package io.github.etr.courses.kafka.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class LogColors {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_GREEN = "\u001B[32m";

    public static String colorizeBlue(String message) {
        return ANSI_BLUE + message + ANSI_RESET;
    }

    public static String colorizeGreen(String message) {
        return ANSI_GREEN + message + ANSI_RESET;
    }
}
