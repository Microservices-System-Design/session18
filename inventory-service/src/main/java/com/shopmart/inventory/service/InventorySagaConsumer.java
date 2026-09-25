package com.shopmart.inventory.service;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventorySagaConsumer {

    private final ProductService productService;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "inventory-group")
    public void handleOrderEvent(OrderEvent event) {
        if (event.getType() == SagaEventType.ORDER_CREATED) {
            try {
                productService.decreaseStock(event.getProductId(), event.getQuantity());
                kafkaTemplate.send(KafkaTopics.ORDER, OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.INVENTORY_RESERVED)
                        .build());
            } catch (Exception e) {
                log.error("Stock deduction failed for orderId={}: {}", event.getOrderId(), e.getMessage());
                kafkaTemplate.send(KafkaTopics.ORDER, OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.INVENTORY_FAILED)
                        .message(e.getMessage())
                        .build());
            }
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            log.info("Rollback: Restoring stock for orderId={}, productId={}, quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            productService.increaseStock(event.getProductId(), event.getQuantity());
            kafkaTemplate.send(KafkaTopics.ORDER, OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_RELEASED)
                    .build());
        }
    }
}
