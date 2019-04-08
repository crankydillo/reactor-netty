package reactor.netty.http.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import reactor.core.publisher.Flux;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.context.Context;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class App {

    public static void main(String[] args) throws Exception {
        SelfSignedCertificate cert = new SelfSignedCertificate();
        SslContext ctx = SslContextBuilder.forServer(cert.certificate(), cert.privateKey()).build();

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

        HttpClient httpClient = HttpClient.create(ConnectionProvider.fixed("fixed", 2))
                .observe(new ConnectionObserver() {
                    // While we can do some logging here, it's not clear how to leverage
                    // the context (and get access to our log transaction) from this client code.
                    // However, the ConnectionObserver and its context play an important role
                    // when it comes to firing the doOnXyz events below.
                    @Override
                    public void onStateChange(Connection conn, State newState) {
                        //printWithThread(conn + ", " + newState + ", " + this.currentContext());
                    }
                })
                .tcpConfiguration(cfg -> {
                    return cfg.doOnConnect(b -> {
                        printWithThread("TCP conn attempt");
                    }).doOnConnected(conn -> {
                        printWithThread("TCP conn established");
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
                                        printWithThread("DNS resolution " + address + " took " + t.elapsedMillis() + " ms.");
                                    }
                                }
                            };
                        }
                    }).bootstrap(b -> {
                        BootstrapHandlers.updateConfiguration(b,
                                "sams.ssl.handshake-handler",
                                (conn, channel) -> {
                                    channel.pipeline()
                                            .addLast(
                                                    "sams.ssl.handshake-listener",
                                                    new ChannelInboundHandlerAdapter() {
                                                        @Override
                                                        public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
                                                                throws Exception {
                                                            // Unfortunately, there is no SslHandshakeStarted event.  Maybe that
                                                            // could be added here: https://github.com/netty/netty/blob/78c02aa033f29d848b3d0eeec141b3840a645703/handler/src/main/java/io/netty/handler/ssl/SslHandler.java#L1876
                                                            // That code is what fires the completed event.
                                                            if (evt instanceof SslHandshakeCompletionEvent) {
                                                                printWithThread("SSL Handshake done");
                                                            }
                                                        }
                                                    });
                                });
                        return b;
                    });
                }).secure(ssl -> ssl.sslContext(
                        SslContextBuilder.forClient()
                                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        )
                ).doOnRequest((req, conn) -> {
                    // This isn't firing at the right time..  I typically (always?) see this fire
                    // after doAfterRequest.  https://github.com/reactor/reactor-netty/issues/558
                    Optional.<RequestContext>ofNullable(req.currentContext().get(ContextKeys.REQUEST_CONTEXT))
                            .ifPresent(reqCtx -> reqCtx.setRequestTrans(new Transaction("CONN")));
                }).doAfterRequest((req, conn) -> {
                    Optional.<RequestContext>ofNullable(req.currentContext().get(ContextKeys.REQUEST_CONTEXT))
                            .flatMap(rc -> Optional.ofNullable(rc.getRequestTrans()))
                            .ifPresent(reqT -> {
                                reqT.complete();
                                printWithThread("Request send took " + reqT.elapsedMillis() + " ms.");
                            });
                }).doOnResponse((resp, conn) -> {
                    Optional.<RequestContext>ofNullable(resp.currentContext().get(ContextKeys.REQUEST_CONTEXT))
                            .ifPresent(reqCtx -> reqCtx.setResponseTrans(new Transaction("RECV")));
                }).doAfterResponse((resp, conn) -> {
                    // This does not have access to the correct context.
                    // https://github.com/reactor/reactor-netty/issues/651; however, it appears
                    // they've already fixed it.  Just need a release.
                    Optional.<RequestContext>ofNullable(resp.currentContext().get(ContextKeys.REQUEST_CONTEXT))
                            .flatMap(rc -> Optional.ofNullable(rc.getResponseTrans()))
                            .ifPresent(respT -> {
                                respT.complete();
                                printWithThread("Response receive took " + respT.elapsedMillis() + " ms.");
                            });
                }).baseUrl("https://localhost:8080");

        List<String> responses =
                Flux.just(1, 2, 3).flatMap(i -> {
                    return httpClient.request(HttpMethod.GET)
                            .uri("/foo")
                            .responseContent()
                            .asString()
                            .subscriberContext(Context.of(ContextKeys.REQUEST_CONTEXT, new RequestContext()))
                            .subscriberContext(c -> c.put("hi", "yes"))
                            .subscriberContext(Context.of("foo", "bar"));

                }).collectList()
                .block();

        printWithThread(responses);
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

    private static void printWithThread(Object o) {
        System.out.println(Thread.currentThread().getName() + " - " + o);
    }

}
