package andrei.libraryeventconsumer.config;


import andrei.libraryeventconsumer.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@RequiredArgsConstructor
@Slf4j
public class LibraryEventConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final LibraryEventsService libraryEventsService;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties())));
        factory.setConcurrency(3); //not recommended for cloud environment
        factory.setErrorHandler((thrownException, consumerRecord) -> {
            log.info("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), consumerRecord);
        });
        factory.setRetryTemplate(retryTemplate());
        factory.setRecoveryCallback(retryContext -> {
            if(retryContext.getLastThrowable().getCause() instanceof RecoverableDataAccessException rdae) {
                //Invoke the recovery logic
                log.info("Inside the recoverable logic");

         /*       Arrays.asList(retryContext.attributeNames())
                        .forEach(attribute -> {
                            log.info("Attribute name is : {} ", attribute);
                            log.info("Attribute Value is : {} ", retryContext.getAttribute(attribute));
                        });*/
                ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) retryContext.getAttribute("record");
                libraryEventsService.handleRecovery(consumerRecord);

            } else {
                log.info("Inside the non recoverable logic");
                throw new RuntimeException(retryContext.getLastThrowable().getMessage());
            }

            return null;
        });

        //set the acknoledge mode to MANUAL and uncomment the LibraryEventsConsumerManualOffset bean and comment the LibraryEventConsumer
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    private RetryTemplate retryTemplate() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
//        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
//        simpleRetryPolicy.setMaxAttempts(3);

        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
        //exception will NOT be retried, as per false condition
        exceptionMap.put(IllegalArgumentException.class, false);

        //exception will be retried
        exceptionMap.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionMap, true);

        return simpleRetryPolicy;
    }


}
