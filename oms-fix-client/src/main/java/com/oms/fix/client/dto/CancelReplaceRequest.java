package com.oms.fix.client.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

public record CancelReplaceRequest(
    @NotBlank String origClOrdId,
    @NotBlank String symbol,
    @NotNull  Side side,
    @NotNull  OrderType orderType,
    @NotNull @DecimalMin("0.0") BigDecimal price,
    @NotNull @DecimalMin("0.0") BigDecimal quantity)
{
}
