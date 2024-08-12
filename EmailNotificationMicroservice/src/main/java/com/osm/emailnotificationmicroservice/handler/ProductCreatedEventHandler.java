package com.osm.emailnotificationmicroservice.handler;

import com.osm.core.ProductCreatedEvent;
import com.osm.emailnotificationmicroservice.error.NotRetryableException;
import com.osm.emailnotificationmicroservice.error.RetryableException;
import com.osm.emailnotificationmicroservice.io.ProcessedEventEntity;
import com.osm.emailnotificationmicroservice.io.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private RestTemplate restTemplate;
    private ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler (RestTemplate restTemplate, ProcessedEventRepository processedEventRepository){
        this.restTemplate = restTemplate;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    @KafkaHandler
    public void handle(@Payload   ProductCreatedEvent productCreatedEvent,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey){

        LOGGER.info("Product created event received: {}", productCreatedEvent.getTitle() +
                "with product id: {}", productCreatedEvent.getProductId());

        ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
        if (existingRecord!=null){
            LOGGER.info("Found a duplicate message id: {}", existingRecord.getMessageId());
            return;
        }

        String requestUrl = "http://localhost:8082";
        try {
            ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().value() == HttpStatus.OK.value()){
                LOGGER.info("received response from remote service: " + response.getBody());
            }
        } catch(ResourceAccessException exception){
            LOGGER.error(exception.getMessage());
            throw new RetryableException(exception);
        } catch (HttpServerErrorException exception){
            LOGGER.error(exception.getMessage());
            throw new NotRetryableException(exception);
        } catch (Exception exception){
            LOGGER.error(exception.getMessage());
            throw new NotRetryableException(exception);
        }
        try {
            processedEventRepository.save(new ProcessedEventEntity(
                    messageId,
                    productCreatedEvent.getProductId()
            ));
        } catch (DataIntegrityViolationException exception){
            throw new NotRetryableException(exception);
        }
    }
}
