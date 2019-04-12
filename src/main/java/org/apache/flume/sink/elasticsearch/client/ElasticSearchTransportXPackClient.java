package org.apache.flume.sink.elasticsearch.client;

/*
 * 添加 ES x-pack 客户端。
 * 账号配置方式：在flum-ng 配置中 添加
 * access_new.sinks.s1.client = x-pack-transport   #指定客户端类型
 * access_new.sinks.s1.client.securityUser = elastic:changeme  #设置连接账号密码
 */

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_SECURITY_USER;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SECURITY_USER;

public class ElasticSearchTransportXPackClient implements ElasticSearchClient {

    public static final Logger logger = LoggerFactory.getLogger(ElasticSearchTransportXPackClient.class);

    private InetSocketTransportAddress[] serverAddresses;
    private ElasticSearchEventSerializer serializer;
    private ElasticSearchIndexRequestBuilderFactory indexRequestBuilderFactory;
    private BulkRequestBuilder bulkRequestBuilder;

    private Client client;
    private String clustername;


    private String securityUser = DEFAULT_SECURITY_USER;

    private String sslKeyPath = "";

    private String sslCertificatePath = "";

    private String sslCertificateAuthPath = "";

    /**
     * Transport client for external cluster
     *
     * @param hostNames
     * @param clusterName
     * @param serializer
     */
    public ElasticSearchTransportXPackClient(String[] hostNames, String clusterName,
                                             ElasticSearchEventSerializer serializer) {
        configureHostnames(hostNames);
        this.serializer = serializer;
        openClient(clusterName);
    }

    public ElasticSearchTransportXPackClient(String[] hostNames, String clusterName,
                                             ElasticSearchIndexRequestBuilderFactory indexBuilder) {
        configureHostnames(hostNames);
        this.indexRequestBuilderFactory = indexBuilder;
        openClient(clusterName);
    }

    /**
     * Local transport client only for testing
     *
     * @param indexBuilderFactory
     */
    public ElasticSearchTransportXPackClient(ElasticSearchIndexRequestBuilderFactory indexBuilderFactory) {
        this.indexRequestBuilderFactory = indexBuilderFactory;
        // openLocalDiscoveryClient();
    }

    /**
     * Local transport client only for testing
     *
     * @param serializer
     */
    public ElasticSearchTransportXPackClient(ElasticSearchEventSerializer serializer) {
        this.serializer = serializer;
        // openLocalDiscoveryClient();
    }

    /**
     * Used for testing
     *
     * @param client     ElasticSearch Client
     * @param serializer Event Serializer
     */
    public ElasticSearchTransportXPackClient(Client client, ElasticSearchEventSerializer serializer) {
        this.client = client;
        this.serializer = serializer;
    }

    /**
     * Used for testing
     *
     * @param client                ElasticSearch Client
     * @param requestBuilderFactory Event Serializer
     */
    public ElasticSearchTransportXPackClient(Client client,
                                             ElasticSearchIndexRequestBuilderFactory requestBuilderFactory) throws IOException {
        this.client = client;
        requestBuilderFactory.createIndexRequest(client, null, null, null);
    }

    @VisibleForTesting
    InetSocketTransportAddress[] getServerAddresses() {
        return serverAddresses;
    }

    @VisibleForTesting
    void setBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {
        this.bulkRequestBuilder = bulkRequestBuilder;
    }

    private void configureHostnames(String[] hostNames) {
        logger.warn(Arrays.toString(hostNames));
        serverAddresses = new InetSocketTransportAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String[] hostPort = hostNames[i].trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
                    : ElasticSearchSinkConstants.DEFAULT_PORT;
            serverAddresses[i] = new InetSocketTransportAddress(new InetSocketAddress(host, port));
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
        client = null;
    }

    public void addEvent(Event event, IndexNameBuilder indexNameBuilder, String indexType,
                         long ttlMs) throws Exception {
        if (bulkRequestBuilder == null) {
            logger.info(client == null ? "client是null" : "client 不是null");
            bulkRequestBuilder = client.prepareBulk();
        }

        IndexRequestBuilder indexRequestBuilder;
        if (indexRequestBuilderFactory == null) {

            indexRequestBuilder = client.prepareIndex(indexNameBuilder.getIndexName(event),
                    indexType);

            XContentBuilder bytesstream = (XContentBuilder) serializer.getContentBuilder(event);

            indexRequestBuilder.setSource(bytesstream);

        } else {
            indexRequestBuilder = indexRequestBuilderFactory.createIndexRequest(client,
                    indexNameBuilder.getIndexPrefix(event), indexType, event);
        }

        if (ttlMs > 0) {
            indexRequestBuilder.setTTL(ttlMs);
        }
        bulkRequestBuilder.add(indexRequestBuilder);
    }

    public void execute() throws Exception {
        try {
            BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
            if (bulkResponse.hasFailures()) {

                throw new EventDeliveryException(bulkResponse.buildFailureMessage());
            }
        } finally {
            bulkRequestBuilder = client.prepareBulk();
        }
    }

    /**
     * Open client to elaticsearch cluster
     *
     * @param clusterName
     */
    private void openClient(String clusterName) {
        logger.info("Using ElasticSearch hostnames: {} ", Arrays.toString(serverAddresses));
        clustername = clusterName;
        // x-pack 客户端创建，需要参数，放在 configure() 中构建
    }


    public void configure(Context context) {
        try {
// To change body of implemented methods use File | Settings | File
            // Templates.
            securityUser = context.getString(SECURITY_USER);
            sslKeyPath = context.getString("sslKeyPath");
            sslCertificatePath = context.getString("sslCertificatePath");
            sslCertificateAuthPath = context.getString("sslCertificateAuthPath");
            logger.info("Using ElasticSearch xpack security user: {} ", securityUser);
            logger.info("Using ElasticSearch xpack sslKeyPath: {} ", sslKeyPath);
            logger.info("Using ElasticSearch xpack sslCertificatePath: {} ", sslCertificatePath);
            logger.info("Using ElasticSearch xpack sslCertificateAuthPath: {} ", sslCertificateAuthPath);
            Settings settings = Settings.builder()
                    .put("cluster.name", clustername)
                    .put("xpack.security.transport.ssl.enabled", true)
                    .put("xpack.security.user", securityUser)
                    .put("xpack.ssl.key", sslKeyPath)
                    .put("xpack.ssl.certificate", sslCertificatePath)
                    .put("xpack.ssl.certificate_authorities", sslCertificateAuthPath)
                    .put("client.transport.sniff", true)
                    .build();

            if (client != null) {
                client.close();
            }
            PreBuiltXPackTransportClient transportClient = new PreBuiltXPackTransportClient(settings);
            for (InetSocketTransportAddress host : serverAddresses) {
                transportClient.addTransportAddress(host);
            }
            client = transportClient;
            int nodeSize = transportClient.connectedNodes().size();
            if (nodeSize <= 0) {
                logger.error("连接ES集群失败，请检查ES账号和密码，以及SSL证书是否正确");
                //启动停止。
                System.exit(0);
            }
        } catch (Exception e) {
            logger.error("configure es 失败", e);
            e.printStackTrace();
            logger.error("configure es 失败：" + e);
        }
    }
}