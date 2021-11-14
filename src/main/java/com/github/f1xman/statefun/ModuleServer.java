package com.github.f1xman.statefun;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

import java.net.InetAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static io.undertow.UndertowOptions.ENABLE_HTTP2;

@Slf4j
class ModuleServer {

    static void startRemoteModuleServer(StatefulFunctions statefulFunctions, int port) {
        RequestReplyHandler handler = statefulFunctions.requestReplyHandler();
        Undertow server =
                Undertow.builder()
                        .addHttpListener(port, "0.0.0.0")
                        .setHandler(new UndertowHttpHandler(handler))
                        .setServerOption(ENABLE_HTTP2, true)
                        .build();
        server.start();
    }

    @SneakyThrows
    static String getHostAddress() {
        return InetAddress.getLocalHost().getHostAddress();
    }

    private static final class UndertowHttpHandler implements HttpHandler {

        private final RequestReplyHandler handler;

        UndertowHttpHandler(RequestReplyHandler handler) {
            this.handler = Objects.requireNonNull(handler);
        }

        @Override
        public void handleRequest(HttpServerExchange exchange) {
            exchange.getRequestReceiver().receiveFullBytes(this::onRequestBody);
        }

        private void onRequestBody(HttpServerExchange exchange, byte[] requestBytes) {
            exchange.dispatch();
            CompletableFuture<Slice> future = handler.handle(Slices.wrap(requestBytes));
            future.whenComplete((response, exception) -> onComplete(exchange, response, exception));
        }

        private void onComplete(HttpServerExchange exchange, Slice responseBytes, Throwable ex) {
            if (ex != null) {
                ex.printStackTrace(System.out);
                exchange.getResponseHeaders().put(Headers.STATUS, 500);
                exchange.endExchange();
                return;
            }
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");
            exchange.getResponseSender().send(responseBytes.asReadOnlyByteBuffer());
        }
    }
}