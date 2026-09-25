package com.shopmart.order.service;

import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "order-group")
    public void handleOrderEvent(OrderEvent event) {
        if (event.getType() == SagaEventType.PAYMENT_COMPLETED) {
            orderService.completeOrder(event.getOrderId());
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED || event.getType() == SagaEventType.INVENTORY_FAILED) {
            orderService.cancelOrder(event.getOrderId(), event.getMessage());
        }
    }
}
