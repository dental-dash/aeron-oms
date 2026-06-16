package com.oms.fix.client;

import com.oms.fix.client.dto.CancelOrderRequest;
import com.oms.fix.client.dto.CancelReplaceRequest;
import com.oms.fix.client.dto.PlaceOrderRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Tag(name = "Orders", description = "FIX order submission — responses stream via SSE until the matching ExecutionReport arrives (30 s timeout)")
@RestController
@RequestMapping("/api/v1/orders")
@Validated
public class OrderController {
    private final FixInitiatorService fixInitiatorService;

    public OrderController(final FixInitiatorService fixInitiatorService) {
        this.fixInitiatorService = fixInitiatorService;
    }

    @Operation(
            summary = "Place new order (NOS, 35=D)",
            requestBody = @RequestBody(required = true, content = @Content(schema = @Schema(implementation = PlaceOrderRequest.class))),
            responses = @ApiResponse(responseCode = "200",
                    description = "SSE stream — emits ExecutionReport JSON then closes",
                    content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                            schema = @Schema(type = "string", example = "data:{\"execType\":\"NEW\",\"orderId\":\"abc123\"}\n\n")))
    )
    @PostMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter placeOrder(@org.springframework.web.bind.annotation.RequestBody @Valid final PlaceOrderRequest req) {
        return fixInitiatorService.submitNos(req);
    }

    @Operation(
            summary = "Cancel order (35=F)",
            requestBody = @RequestBody(required = true, content = @Content(schema = @Schema(implementation = CancelOrderRequest.class))),
            responses = @ApiResponse(responseCode = "200", description = "SSE stream — emits ExecutionReport JSON then closes",
                    content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                            schema = @Schema(type = "string", example = "data:{\"execType\":\"CANCELED\",\"orderId\":\"abc123\"}\n\n")))
    )
    @DeleteMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter cancelOrder(@org.springframework.web.bind.annotation.RequestBody @Valid final CancelOrderRequest req) {
        return fixInitiatorService.submitCancel(req);
    }

    @Operation(
            summary = "Cancel-replace order (35=G)",
            requestBody = @RequestBody(required = true, content = @Content(schema = @Schema(implementation = CancelReplaceRequest.class))),
            responses = @ApiResponse(responseCode = "200", description = "SSE stream — emits ExecutionReport JSON then closes",
                    content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE, schema = @Schema(type = "string", example = "data:{\"execType\":\"REPLACED\",\"orderId\":\"abc123\"}\n\n")))
    )
    @PutMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter cancelReplaceOrder(@org.springframework.web.bind.annotation.RequestBody @Valid final CancelReplaceRequest req) {
        return fixInitiatorService.submitCancelReplace(req);
    }
}