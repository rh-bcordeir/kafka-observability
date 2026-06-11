package br.com.redhat.dto;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class OrderDTODeserializer extends ObjectMapperDeserializer<OrderDTO> {
    public OrderDTODeserializer() {
        super(OrderDTO.class);
    }
}
