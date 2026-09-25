package com.shopmart.payment.service;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;

import java.util.List;

public interface PaymentService {

    PaymentResponse processPayment(PaymentRequest request);

    PaymentResponse refund(Long orderId);

    PaymentResponse getByOrderId(Long orderId);

    List<PaymentResponse> getAll();
}
