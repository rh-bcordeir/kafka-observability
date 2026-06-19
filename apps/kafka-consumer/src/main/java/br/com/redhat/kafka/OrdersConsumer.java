package br.com.redhat.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.redhat.dto.OrderDTO;

@ApplicationScoped
public class OrdersConsumer {

    @Incoming("orders")
    public void sink(ConsumerRecord<String, OrderDTO> record) {
        System.out.println(">> " + record.value());
        System.out.println(">> key: " + record.key());
        System.out.println(">> offset: " + record.offset());
        System.out.println(">> partition: " + record.partition() + "\n");
    }
}
