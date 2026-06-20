package com.oms.fix.client.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CancelOrderRequest(
    @NotBlank String origClOrdId,
    @NotBlank String symbol,
    @NotNull  Side side)
{
}
