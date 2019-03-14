package reactor.netty.http.client;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
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
import reactor.util.context.Context;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class App {

    public static void main(String[] args) throws Exception {

        SelfSignedCertificate cert = new SelfSignedCertificate();
        SslContext ctx = SslContextBuilder.forServer(cert.certificate(), cert.privateKey()).build();

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
                .secure(ssl -> ssl.sslContext(ctx))
                .wiretap(true)
                .bindNow();

        HttpClient httpClient = HttpClient.create()
                .tcpConfiguration(cfg -> {
                    // TODO This absolutely won't work..  This is essentially going to make all CONNECTs on for
                    // this client get in line...
                    final AtomicReference<Transaction> t = new AtomicReference<>();
                    return cfg.doOnConnect(b -> {
                        t.set(new Transaction("CONN"));
                    }).doOnConnected(client -> {
                        t.get().complete();
                        System.out.println("TCP connect took " + t.get().elapsedMillis() + " ms.");
                    }).resolver(new AddressResolverGroup<InetSocketAddress>() {
                        // hack coupling:(
                        private final AddressResolverGroup<InetSocketAddress> delegate = DefaultAddressResolverGroup.INSTANCE;

                        @Override
                        protected AddressResolver<InetSocketAddress> newResolver(EventExecutor executor) {
                            // Possible hack??  I'd feel much better about implementing `getResolver`
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
                                    Transaction t = new Transaction("time");
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
                }).secure(ssl -> ssl.sslContext(SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE))
                )
                .doOnRequest((req, conn) -> {
                    RequestContext reqCtx = req.currentContext().get(ContextKeys.REQUEST_CONTEXT);
                    reqCtx.setRequestTrans(new Transaction("CONN"));
                }).doAfterRequest((req, conn) -> {
                    Optional.<RequestContext>ofNullable(req.currentContext().get(ContextKeys.REQUEST_CONTEXT))
                            .flatMap(rc -> Optional.ofNullable(rc.getRequestTrans()))
                            .ifPresent(reqT -> {
                                reqT.complete();
                                System.out.println("Request send took " + reqT.elapsedMillis() + " ms.");
                            });
                }).doOnResponse((resp, conn) -> {
                    // this is fired after headers received.  Is that good enough?
                    RequestContext reqCtx = resp.currentContext().get(ContextKeys.REQUEST_CONTEXT);
                    reqCtx.setResponseTrans(new Transaction("RECV"));
                }).doAfterResponse((resp, conn) -> {
                    Optional.<RequestContext>ofNullable(resp.currentContext().get(ContextKeys.REQUEST_CONTEXT))
                            .flatMap(rc -> Optional.ofNullable(rc.getResponseTrans()))
                            .ifPresent(reqT -> {
                                reqT.complete();
                                System.out.println("Response send took " + reqT.elapsedMillis() + " ms.");
                            });
                }).baseUrl("https://localhost:8080");

        Supplier<String> httpGet = () -> {
            return httpClient.request(HttpMethod.GET)
                    .uri("/foo")
                    .responseSingle((r, buf) -> buf.asString())
                    .subscriberContext(context -> context.put(ContextKeys.REQUEST_CONTEXT, new RequestContext()))
                    .block();
        };

        System.out.println(httpGet.get());
        System.out.println(httpGet.get());
        System.out.println(httpGet.get());
    }

    static class ContextKeys {
        static final String REQUEST_CONTEXT = "REQ_CTX";
    }

    // I find it ironic that I've pointed out the flaws in mutating beans
    // via a Flux-like process when I write code like this!
    static class RequestContext {
        private Transaction requestTrans;
        private Transaction responseTrans;

        public Transaction getRequestTrans() {
            return requestTrans;
        }

        public void setRequestTrans(Transaction requestTrans) {
            if (this.requestTrans != null) throw new RuntimeException("You can only set this once you bad boy!");
            this.requestTrans = Objects.requireNonNull(requestTrans);
        }

        public Transaction getResponseTrans() {
            return responseTrans;
        }

        public void setResponseTrans(Transaction responseTrans) {
            if (this.responseTrans != null) throw new RuntimeException("You can only set this once you bad boy!");
            this.responseTrans = Objects.requireNonNull(responseTrans);
        }

    }

    static class Transaction {
        private final String name;
        private final long startNs;
        private Long completedNs;

        public Transaction(String name) {
            this.name = Objects.requireNonNull(name);
            this.startNs = System.nanoTime();
        }

        public void complete() {
            if (completedNs == null) {
                completedNs = System.nanoTime();
            }
        }

        public long elapsedMillis() {
            Function<Long, Long> nanosToMillis = ms -> ms / 1_000_000;
            if (completedNs == null) {
                return nanosToMillis.apply(System.nanoTime() - startNs);
            }
            return nanosToMillis.apply(completedNs - startNs);
        }
    }

}
