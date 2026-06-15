package com.oms.fix.client;

import com.oms.fix.client.dto.CancelOrderRequest;
import com.oms.fix.client.dto.CancelReplaceRequest;
import com.oms.fix.client.dto.PlaceOrderRequest;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * REST entry-points for FIX order submission.
 *
 * <p>Each endpoint returns an {@link SseEmitter}; the response stream stays open until
 * the matching ExecutionReport arrives (or the 30-second timeout fires).
 *
 * <p>Verb rationale:
 * <ul>
 *   <li>POST — new order (NOS, 35=D)
 *   <li>DELETE — cancel (OrderCancelRequest, 35=F); body carries the identifiers
 *   <li>PUT — cancel-replace (OrderCancelReplaceRequest, 35=G)
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/orders")
@Validated
public class OrderController {
    private final FixInitiatorService fixInitiatorService;

    public OrderController(final FixInitiatorService fixInitiatorService) {
        this.fixInitiatorService = fixInitiatorService;
    }

    @PostMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter placeOrder(@RequestBody @Valid final PlaceOrderRequest req) {
        return fixInitiatorService.submitNos(req);
    }

    @DeleteMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter cancelOrder(@RequestBody @Valid final CancelOrderRequest req) {
        return fixInitiatorService.submitCancel(req);
    }

    @PutMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter cancelReplaceOrder(@RequestBody @Valid final CancelReplaceRequest req) {
        return fixInitiatorService.submitCancelReplace(req);
    }
}