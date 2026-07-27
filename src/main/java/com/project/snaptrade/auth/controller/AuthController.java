package com.project.snaptrade.auth.controller;

import com.project.snaptrade.auth.dto.LoginRequestDto;
import com.project.snaptrade.auth.dto.SignupRequestDto;
import com.project.snaptrade.auth.service.AuthService;
import com.project.snaptrade.common.response.CommonSuccessDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/signup")
    public ResponseEntity<CommonSuccessDto<Long>> signup(@RequestBody SignupRequestDto request) {
        Long userId = authService.signup(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(CommonSuccessDto.created(userId));
    }

    @PostMapping("/login")
    public ResponseEntity<CommonSuccessDto<String>> login(@RequestBody LoginRequestDto request) {
        String token = authService.login(request);
        return ResponseEntity.ok(CommonSuccessDto.ok(token));
    }
}
