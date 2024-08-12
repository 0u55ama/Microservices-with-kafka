package com.osm.productmicroservice.service;
import com.osm.productmicroservice.rest.CreateProductRestModel;

public interface ProductService {
    String createProduct(CreateProductRestModel product) throws Exception ;
}
