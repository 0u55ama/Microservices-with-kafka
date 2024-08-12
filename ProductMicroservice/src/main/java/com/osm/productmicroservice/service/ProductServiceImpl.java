package com.osm.productmicroservice.service;

import com.osm.core.ProductCreatedEvent;
import com.osm.productmicroservice.rest.CreateProductRestModel;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ProductServiceImpl implements ProductService {

    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private  final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    @Override
    public String createProduct(CreateProductRestModel product) throws Exception {
        String productId = UUID.randomUUID().toString();

        //TODO: persist product details in database

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
                                                                            product.getTitle(),
                                                                            product.getPrice(),
                                                                            product.getQuantity());

//        kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent);

//        SendResult<String, ProductCreatedEvent> result =
//                kafkaTemplate.send("product-created-events-topic",productId, productCreatedEvent).get();


//        future.whenComplete((result, exception) -> { results
//            if (exception != null) {
//                LOG.error("**************** Message sent failed : " + exception.getMessage() + "****************");
//            } else {
//                LOG.info("**************** Message sent successfully : " + result.getRecordMetadata() + "****************");
//            }
//        });

//        future.join();


        LOGGER.info("Before publishing a ProductCreatedEvent");

        ProducerRecord<String,ProductCreatedEvent> record = new ProducerRecord<>(
                "product-created-events-topic",
                productId,
                productCreatedEvent
        );
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        SendResult<String, ProductCreatedEvent> result =
                kafkaTemplate.send(record).get();

        LOGGER.info("Partition: " + result.getRecordMetadata().partition());
        LOGGER.info("Topic: " + result.getRecordMetadata().topic());
        LOGGER.info("Offset: " + result.getRecordMetadata().offset());

        LOGGER.info("***** Returning product id");

        return productId;
    }
}
