package com.shopmart.payment.service;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.KafkaTopics;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentSagaConsumerTest {

    @Mock
    private PaymentService paymentService;

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @InjectMocks
    private PaymentSagaConsumer sagaConsumer;

    @Test
    void handleOrderEvent_shouldSendPaymentCompleted_whenPaymentSuccess() {
        OrderEvent event = OrderEvent.builder()
                .orderId(1L)
                .productId(2L)
                .quantity(1)
                .amount(new BigDecimal("10000000"))
                .type(SagaEventType.INVENTORY_RESERVED)
                .build();

        PaymentResponse response = PaymentResponse.builder()
                .orderId(1L)
                .amount(new BigDecimal("10000000"))
                .status(PaymentStatus.SUCCESS)
                .build();

        when(paymentService.processPayment(any(PaymentRequest.class))).thenReturn(response);

        sagaConsumer.handleOrderEvent(event);

        ArgumentCaptor<OrderEvent> captor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(kafkaTemplate).send(eq(KafkaTopics.ORDER), captor.capture());
        assertThat(captor.getValue().getType()).isEqualTo(SagaEventType.PAYMENT_COMPLETED);
    }

    @Test
    void handleOrderEvent_shouldSendPaymentFailed_whenPaymentFails() {
        OrderEvent event = OrderEvent.builder()
                .orderId(1L)
                .productId(2L)
                .quantity(1)
                .amount(new BigDecimal("90000000"))
                .type(SagaEventType.INVENTORY_RESERVED)
                .build();

        PaymentResponse response = PaymentResponse.builder()
                .orderId(1L)
                .amount(new BigDecimal("90000000"))
                .status(PaymentStatus.FAILED)
                .message("Over limit")
                .build();

        when(paymentService.processPayment(any(PaymentRequest.class))).thenReturn(response);

        sagaConsumer.handleOrderEvent(event);

        ArgumentCaptor<OrderEvent> captor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(kafkaTemplate).send(eq(KafkaTopics.ORDER), captor.capture());
        assertThat(captor.getValue().getType()).isEqualTo(SagaEventType.PAYMENT_FAILED);
    }
}
