package com.shopmart.order.service;

import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;

import java.util.List;

public interface OrderService {

    OrderResponse createOrder(OrderRequest request);

    OrderResponse getOrderById(Long id);

    List<OrderResponse> getAllOrders();

    OrderResponse completeOrder(Long orderId);

    OrderResponse cancelOrder(Long orderId, String reason);

    ProductDto fetchProduct(Long productId);
}
