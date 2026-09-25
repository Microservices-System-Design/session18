package com.shopmart.inventory.service;

import com.shopmart.inventory.dto.ProductRequest;
import com.shopmart.inventory.dto.ProductResponse;

import java.util.List;

public interface ProductService {

    List<ProductResponse> getAllProducts();

    ProductResponse getProductById(Long id);

    ProductResponse createProduct(ProductRequest request);

    ProductResponse updateProduct(Long id, ProductRequest request);

    void deleteProduct(Long id);

    ProductResponse decreaseStock(Long id, int quantity);

    ProductResponse increaseStock(Long id, int quantity);
}
