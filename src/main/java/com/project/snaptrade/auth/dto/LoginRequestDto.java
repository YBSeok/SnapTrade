package com.project.snaptrade.auth.dto;

public record LoginRequestDto(
        String email,
        String password
) {
}