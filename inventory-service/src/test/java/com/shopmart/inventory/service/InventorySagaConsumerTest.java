package com.shopmart.inventory.service;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class InventorySagaConsumerTest {

    @Mock
    private ProductService productService;

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @InjectMocks
    private InventorySagaConsumer sagaConsumer;

    @Test
    void handleOrderEvent_shouldDecreaseStock_whenOrderCreated() {
        OrderEvent event = OrderEvent.builder()
                .orderId(1L)
                .productId(2L)
                .quantity(3)
                .amount(new BigDecimal("30000000"))
                .type(SagaEventType.ORDER_CREATED)
                .build();

        sagaConsumer.handleOrderEvent(event);

        verify(productService).decreaseStock(2L, 3);
        ArgumentCaptor<OrderEvent> captor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(kafkaTemplate).send(eq(KafkaTopics.ORDER), captor.capture());
        assertThat(captor.getValue().getType()).isEqualTo(SagaEventType.INVENTORY_RESERVED);
    }

    @Test
    void handleOrderEvent_shouldRestoreStock_whenPaymentFailed() {
        OrderEvent event = OrderEvent.builder()
                .orderId(1L)
                .productId(2L)
                .quantity(3)
                .type(SagaEventType.PAYMENT_FAILED)
                .build();

        sagaConsumer.handleOrderEvent(event);

        verify(productService).increaseStock(2L, 3);
        ArgumentCaptor<OrderEvent> captor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(kafkaTemplate).send(eq(KafkaTopics.ORDER), captor.capture());
        assertThat(captor.getValue().getType()).isEqualTo(SagaEventType.INVENTORY_RELEASED);
    }
}
