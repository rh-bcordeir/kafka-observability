package br.com.redhat.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

import br.com.redhat.dto.OrderDTO;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

public class OrdersProducer {

    @Channel("orders")
    Emitter<OrderDTO> emitter;

    private static final Random RANDOM = new Random();

    public static final List<String> TITLES = Arrays.asList("Laptop", "Phone", "Headset", "Keyboard", "Monitor", "Mouse", "Webcam");

    public void sendMessage(OrderDTO message){
        System.out.println("===== Start sending ====");
        try {
            emitter.send(
                Message.of(message)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                        .withKey(message.key())
                        .build()
                    )
                    .withAck(() -> {
                        System.out.println("Broker confirmou o recebimento");
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(throwable -> {
                        System.out.println("Falha ao enviar: " + throwable.getMessage());
                        return CompletableFuture.completedFuture(null);
                    })
            );
            System.out.println("========================");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendLoopMessage() throws InterruptedException {
        for (int i = 0; i < 1_000_000; i++) {

            String title = TITLES.get(RANDOM.nextInt(TITLES.size()));
            var message = new OrderDTO(
                null,
                title,
                title + " " + String.valueOf(i),
                RANDOM.nextInt(1, 101)
            );
            sendMessage(message);    
            Thread.sleep(500);
        }
    }    
}