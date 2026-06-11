package br.com.redhat.kafka;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.redhat.dto.OrderDTO;

import java.util.stream.Stream;

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
