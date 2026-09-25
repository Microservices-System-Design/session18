package com.shopmart.payment.service;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.KafkaTopics;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSagaConsumer {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "payment-group")
    public void handleOrderEvent(OrderEvent event) {
        if (event.getType() == SagaEventType.INVENTORY_RESERVED) {
            try {
                PaymentResponse response = paymentService.processPayment(
                        new PaymentRequest(event.getOrderId(), event.getAmount()));

                if (response.getStatus() == PaymentStatus.SUCCESS) {
                    kafkaTemplate.send(KafkaTopics.ORDER, OrderEvent.builder()
                            .orderId(event.getOrderId())
                            .productId(event.getProductId())
                            .quantity(event.getQuantity())
                            .amount(event.getAmount())
                            .type(SagaEventType.PAYMENT_COMPLETED)
                            .build());
                } else {
                    kafkaTemplate.send(KafkaTopics.ORDER, OrderEvent.builder()
                            .orderId(event.getOrderId())
                            .productId(event.getProductId())
                            .quantity(event.getQuantity())
                            .amount(event.getAmount())
                            .type(SagaEventType.PAYMENT_FAILED)
                            .message(response.getMessage())
                            .build());
                }
            } catch (Exception e) {
                log.error("Payment error for orderId={}: {}", event.getOrderId(), e.getMessage());
                kafkaTemplate.send(KafkaTopics.ORDER, OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.PAYMENT_FAILED)
                        .message(e.getMessage())
                        .build());
            }
        }
    }
}
