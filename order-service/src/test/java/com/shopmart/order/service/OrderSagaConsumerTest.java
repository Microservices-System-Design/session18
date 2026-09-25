package com.shopmart.order.service;

import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderSagaConsumerTest {

    @Mock
    private OrderService orderService;

    @InjectMocks
    private OrderSagaConsumer orderSagaConsumer;

    @Test
    void handleOrderEvent_shouldCancelOrder_whenPaymentFailed() {
        OrderEvent event = OrderEvent.builder()
                .orderId(10L)
                .productId(1L)
                .quantity(2)
                .amount(new BigDecimal("50000000"))
                .type(SagaEventType.PAYMENT_FAILED)
                .message("Payment gateway failed")
                .build();

        orderSagaConsumer.handleOrderEvent(event);

        verify(orderService).cancelOrder(10L, "Payment gateway failed");
    }

    @Test
    void handleOrderEvent_shouldCompleteOrder_whenPaymentCompleted() {
        OrderEvent event = OrderEvent.builder()
                .orderId(10L)
                .productId(1L)
                .quantity(2)
                .amount(new BigDecimal("50000000"))
                .type(SagaEventType.PAYMENT_COMPLETED)
                .build();

        orderSagaConsumer.handleOrderEvent(event);

        verify(orderService).completeOrder(10L);
    }
}
