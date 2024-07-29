package com.bfi.riskmanagement.services;

import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Service
public class TypeChecker {

    // Method to check if the value represents a BIGINT
    public static boolean isBigInt(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Method to check if the value represents a DOUBLE
    public static boolean isDouble(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Method to check if the value represents a BOOLEAN
    public static boolean isBoolean(String value) {
        return value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false");
    }

    public static boolean isDate(String value) {
        try {
            // Define the expected date format
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            // Attempt to parse the string as a date
            LocalDate.parse(value, dateFormatter);
            // If parsing succeeds, return true
            return true;
        } catch (DateTimeParseException e) {
            // If parsing fails, return false
            return false;
        }
    }
}
