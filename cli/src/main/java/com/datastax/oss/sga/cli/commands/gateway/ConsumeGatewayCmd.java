package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.websocket.WebSocketClient;
import jakarta.websocket.CloseReason;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "consume",
        description = "Consume messages from a gateway")
public class ConsumeGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;
    @CommandLine.Parameters(description = "Gateway ID")
    private String gatewayId;

    @Override
    @SneakyThrows
    public void run() {
        final String consumePath = "%s/v1/consume/%s/%s/%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, gatewayId);

        CountDownLatch latch = new CountDownLatch(1);
        try (final WebSocketClient client = new WebSocketClient(new WebSocketClient.Handler() {
            @Override
            public void onMessage(String msg) {
                log(msg);
            }

            @Override
            public void onClose(CloseReason closeReason) {
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                log("Connection error: %s".formatted(throwable.getMessage()));
                latch.countDown();
            }
        })
                .connect(URI.create(consumePath))) {
            log("Connected to %s".formatted(consumePath));
            latch.await();
        }
    }
}
