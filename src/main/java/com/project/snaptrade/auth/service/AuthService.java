package com.project.snaptrade.auth.service;

import com.project.snaptrade.auth.domain.User;
import com.project.snaptrade.auth.dto.LoginRequestDto;
import com.project.snaptrade.auth.dto.SignupRequestDto;
import com.project.snaptrade.auth.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    @Transactional
    public Long signup(SignupRequestDto request) {
        if (userRepository.existsByEmail(request.email())) {
            throw new IllegalArgumentException("이미 존재하는 이메일입니다.");
        }

        String encodedPassword = passwordEncoder.encode(request.password());

        User newUser = User.builder()
                .name(request.name())
                .email(request.email())
                .password(encodedPassword)
                .status("ACTIVE")
                .build();

        return userRepository.save(newUser).getId();
    }

    @Transactional(readOnly = true)
    public String login(LoginRequestDto request) {
        User user = userRepository.findByEmail(request.email())
                .orElseThrow(() -> new IllegalArgumentException("이메일 또는 비밀번호가 잘못되었습니다."));

        if (!passwordEncoder.matches(request.password(), user.getPassword())) {
            throw new IllegalArgumentException("이메일 또는 비밀번호가 잘못되었습니다.");
        }

        return "Authenticated_User_" + user.getId();
    }
}
