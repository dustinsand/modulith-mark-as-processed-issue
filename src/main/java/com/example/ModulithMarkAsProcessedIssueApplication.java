package com.example;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.javamoney.moneta.Money;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class ModulithMarkAsProcessedIssueApplication {

    public static void main(String[] args) {
        SpringApplication.run(ModulithMarkAsProcessedIssueApplication.class, args);
    }
}

record SomeEvent(@JsonDeserialize(using = MoneyDeserializer.class) Money amount) {}

@Service
class SomeService {
    private final ApplicationEventPublisher publisher;

    SomeService(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Transactional
    public void doSomething() {
        publisher.publishEvent(new SomeEvent(Money.of(BigInteger.TEN, "EUR")));
    }

}

@Component
class SomeEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SomeEventListener.class);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final AtomicBoolean success = new AtomicBoolean(false);

    @ApplicationModuleListener
    void handle(SomeEvent event) {
        if (counter.getAndIncrement() == 0) {
            throw new RuntimeException("fail to process");
        }
        LOGGER.info("Processed successfully: {}", event);
        success.set(true);
    }

    public AtomicInteger getCounter() {
        return counter;
    }

    public boolean isSuccess() {
        return success.get();
    }
}

class MoneyDeserializer extends JsonDeserializer<Money> {

    @Override
    public Money deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        JsonNode jsonNode = ctxt.readValue(p, JsonNode.class);
        String currencyCode = jsonNode.get("currency").get("currencyCode").asText();
        double number = jsonNode.get("number").asDouble();
        return Money.of(number, currencyCode);
    }
}