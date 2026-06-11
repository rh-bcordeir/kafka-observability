package br.com.redhat.resources;

import br.com.redhat.dto.OrderDTO;
import br.com.redhat.kafka.OrdersProducer;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

@Path("/orders")
public class OrdersResource {

    @Inject
    OrdersProducer myTopicProducer;

    @POST
    public Response sendMessage(OrderDTO message) {
        myTopicProducer.sendMessage(message);
        return Response.ok().build();
    }
}
