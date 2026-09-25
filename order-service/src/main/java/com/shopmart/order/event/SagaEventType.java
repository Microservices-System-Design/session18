package com.shopmart.order.event;

public enum SagaEventType {
    ORDER_CREATED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    INVENTORY_RELEASED
}
