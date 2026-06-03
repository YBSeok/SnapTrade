package com.project.snaptrade.engine.Dto;

import java.math.BigDecimal;

public record MatchResult(
        BigDecimal fillQty,
        BigDecimal fillPrice
) {

};