package br.com.redhat.kafka;

import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.redhat.dto.OrderDTO;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.inject.Inject;

public class OrdersProducer {

    @Channel("orders")
    Emitter<OrderDTO> emitter;

    @ConfigProperty(name = "mp.messaging.outgoing.orders.bootstrap.servers")
    String bootstrapServers;
    
    @Inject
    ObjectMapper objectMapper;

    public void sendMessage(OrderDTO message){
        System.out.println("===== Start sending ====");
        try {
            emitter.send(
                Message.of(message)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                    .withKey(message.key())
                    .build()).withAck(() -> {
                        System.out.println("Broker confirmou o recebimento");
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(throwable -> {
                        System.out.println("Falha ao enviar: " + throwable.getMessage());
                        return CompletableFuture.completedFuture(null);
                    })
            );
            System.out.println("Message sent to Kafka");
            System.out.println("========================");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }    
}