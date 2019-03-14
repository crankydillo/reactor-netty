package reactor.netty.http.client;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class App {


    public static void main(String[] args) throws Exception {

        System.out.println("Hi");
        int port = 8080;
        HttpServer.create()
                .port(port)
                .route(r ->
                        r.get("/foo",
                                (req, resp) -> {
                                    return resp.status(200)
                                            .sendString(Flux.just("hello"));
                                }))
                .wiretap(true)
                .bindNow();

        HttpClient httpClient = HttpClient.create()
                .tcpConfiguration(cfg -> {
                    final AtomicReference<Transaction> t = new AtomicReference<>();
                    return cfg.doOnConnect(b -> {
                        t.set(new Transaction());
                    }).doOnConnected(client -> {
                        t.get().complete();
                        System.out.println("TCP connect took " + t.get().elapsedMillis() + " ms.");
                    }).resolver(new AddressResolverGroup<InetSocketAddress>() {
                        private final AddressResolverGroup<InetSocketAddress> delegate = DefaultAddressResolverGroup.INSTANCE;

                        @Override
                        protected AddressResolver<InetSocketAddress> newResolver(EventExecutor executor) {
                            // definitely not sure about this one..
                            final AddressResolver<InetSocketAddress> resolverDelegate = delegate.getResolver(executor);

                            return new AddressResolver<InetSocketAddress>() {
                                @Override
                                public boolean isSupported(SocketAddress address) {
                                    return resolverDelegate.isSupported(address);
                                }

                                @Override
                                public boolean isResolved(SocketAddress address) {
                                    return resolverDelegate.isResolved(address);
                                }

                                @Override
                                public Future<InetSocketAddress> resolve(SocketAddress address) {
                                    return time(address, () -> resolverDelegate.resolve(address));
                                }

                                @Override
                                public Future<InetSocketAddress> resolve(SocketAddress address, Promise<InetSocketAddress> promise) {
                                    return time(address, () -> resolverDelegate.resolve(address, promise));
                                }

                                @Override
                                public Future<List<InetSocketAddress>> resolveAll(SocketAddress address) {
                                    return time(address, () -> resolverDelegate.resolveAll(address));
                                }

                                @Override
                                public Future<List<InetSocketAddress>> resolveAll(SocketAddress address, Promise<List<InetSocketAddress>> promise) {
                                    return time(address, () -> resolverDelegate.resolveAll(address, promise));
                                }

                                @Override
                                public void close() {
                                    resolverDelegate.close();
                                }

                                private <T> T time(SocketAddress address, Supplier<T> fn) {
                                    Transaction t = new Transaction();
                                    try {
                                        return fn.get();
                                    } finally {
                                        t.complete();
                                        System.out.println("DNS resolution " + address + " took " + t.elapsedMillis() + " ms.");
                                    }
                                }
                            };
                        }
                    });
                })
                .baseUrl("http://localhost:8080");

        String resp = httpClient.request(HttpMethod.GET)
                .uri("/foo")
                .responseSingle((r, buf) -> buf.asString())
                .block();

        System.out.println(resp);
    }

    static class Transaction {
        private final long startNs;
        private Long completedNs;

        public Transaction() {
            this.startNs = System.nanoTime();
        }

        public void complete() {
            if (completedNs == null) {
                completedNs = System.nanoTime();
            }
        }

        public long elapsedMillis() {
            return (completedNs - startNs) / 1_000_000;
        }
    }

}
