package dev.kpulse.it;

import dev.kpulse.KafkaProtocolHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * An in-process Pulsar broker over {@link PulsarMockBookKeeper} and an in-memory metadata store, with
 * the kpulse handler bound to a real Netty listener. Mirrors KoP's {@code KopProtocolHandlerTestBase}
 * pattern adapted to upstream Pulsar 5.0.0-M1: the {@code pulsar-broker} test jar is not published, so
 * the mock BookKeeper is injected by subclassing {@link PulsarService} rather than via Mockito.
 */
final class EmbeddedPulsarKafka implements AutoCloseable {

    private static final String CLUSTER = "test";
    private static final String INTERCEPTOR =
        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor";

    private final int kafkaPort;
    private final OrderedExecutor bookKeeperExecutor;
    private final PulsarMockBookKeeper mockBookKeeper;
    private final PulsarService pulsar;
    private final KafkaProtocolHandler handler;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Channel serverChannel;

    EmbeddedPulsarKafka() throws Exception {
        this(false);
    }

    EmbeddedPulsarKafka(boolean deduplicationEnabled) throws Exception {
        this.kafkaPort = freePort();
        this.bookKeeperExecutor = OrderedScheduler.newSchedulerBuilder()
            .numThreads(1).name("kpulse-mock-bk").build();
        this.mockBookKeeper = new PulsarMockBookKeeper(bookKeeperExecutor);

        ServiceConfiguration config = brokerConfig(kafkaPort, deduplicationEnabled);
        this.pulsar = startBroker(config, mockBookKeeper);
        bootstrapClusterMetadata(pulsar);

        this.handler = new KafkaProtocolHandler();
        handler.initialize(config);
        handler.start(pulsar.getBrokerService());

        Map<InetSocketAddress, ChannelInitializer<SocketChannel>> initializers = handler.newChannelInitializers();
        ChannelInitializer<SocketChannel> initializer = initializers.values().iterator().next();

        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
        this.serverChannel = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(initializer)
            .bind("127.0.0.1", kafkaPort)
            .sync()
            .channel();
    }

    String bootstrapServers() {
        return "127.0.0.1:" + kafkaPort;
    }

    int kafkaPort() {
        return kafkaPort;
    }

    PulsarService pulsar() {
        return pulsar;
    }

    private static ServiceConfiguration brokerConfig(int kafkaPort, boolean deduplicationEnabled) {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName(CLUSTER);
        config.setAdvertisedAddress("127.0.0.1");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));

        String metadataUrl = "memory:" + UUID.randomUUID();
        config.setMetadataStoreUrl(metadataUrl);
        config.setConfigurationMetadataStoreUrl(metadataUrl);

        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setBrokerEntryMetadataInterceptors(Set.of(INTERCEPTOR));
        config.setBrokerDeduplicationEnabled(deduplicationEnabled);
        config.setAllowAutoTopicCreation(true);
        // One bundle per namespace so this single broker owns the whole key range up front; otherwise
        // getOrCreateTopic can hit a bundle this broker has not yet acquired ("please redo the lookup").
        config.setDefaultNumberOfNamespaceBundles(1);
        config.setBrokerShutdownTimeoutMs(0L);

        config.getProperties().setProperty("kafkaListeners", "PLAINTEXT://127.0.0.1:" + kafkaPort);
        config.getProperties().setProperty("kafkaAdvertisedListeners", "PLAINTEXT://127.0.0.1:" + kafkaPort);
        return config;
    }

    private static PulsarService startBroker(ServiceConfiguration config, PulsarMockBookKeeper mockBookKeeper)
            throws Exception {
        PulsarService pulsar = new PulsarService(config) {
            @Override
            public BookKeeperClientFactory newBookKeeperClientFactory() {
                return new BookKeeperClientFactory() {
                    @Override
                    public CompletableFuture<BookKeeper> create(ServiceConfiguration conf, MetadataStoreExtended store,
                            EventLoopGroup eventLoopGroup,
                            Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                            Map<String, Object> properties) {
                        return CompletableFuture.completedFuture(mockBookKeeper);
                    }

                    @Override
                    public CompletableFuture<BookKeeper> create(ServiceConfiguration conf, MetadataStoreExtended store,
                            EventLoopGroup eventLoopGroup,
                            Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                            Map<String, Object> properties, StatsLogger statsLogger) {
                        return CompletableFuture.completedFuture(mockBookKeeper);
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
        pulsar.start();
        return pulsar;
    }

    private static void bootstrapClusterMetadata(PulsarService pulsar) throws Exception {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build()) {
            admin.clusters().createCluster(CLUSTER, ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .brokerServiceUrl(pulsar.getBrokerServiceUrl())
                .build());
            admin.tenants().createTenant("public",
                TenantInfo.builder().allowedClusters(Set.of(CLUSTER)).build());
            admin.namespaces().createNamespace("public/default");
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @Override
    public void close() throws Exception {
        if (serverChannel != null) {
            serverChannel.close().sync();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (handler != null) {
            handler.close();
        }
        if (pulsar != null) {
            pulsar.close();
        }
        if (mockBookKeeper != null) {
            mockBookKeeper.shutdown();
        }
        if (bookKeeperExecutor != null) {
            bookKeeperExecutor.shutdown();
        }
    }
}
