package com.project.snaptrade.auth.dto;

public record SignupRequestDto(
        String name,
        String email,
        String password
) {
}

