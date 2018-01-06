package com.zouyyu.demo.es;

import static org.apache.commons.lang.StringUtils.*;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.net.InetAddress;

/**
 * @author YuZou
 * @date 05/01/2018
 */
@Configuration
public class TransportClientFactoryBean implements FactoryBean<TransportClient>, InitializingBean, DisposableBean {


    public static final Logger logger = LoggerFactory.getLogger(TransportClientFactoryBean.class);

    @Value("${spring.data.elasticsearch.cluster-nodes}")
    private String clusterNodes = "127.0.0.1:8300";
    @Value("${spring.data.elasticsearch.cluster-name}")
    private String clusterName  = "elasticsearch";
    private Boolean clientTransportSniff = true;
    private Boolean clientIgnoreClusterName = Boolean.FALSE;
    private String clientPingTimeout = "5s";
    private String clientNodesSamplerInterval = "5s";
    private TransportClient client;
    static final String COLON = ":";
    static final String COMMA = ";";


    public void destroy() throws Exception {
        try {
            logger.info("Closing elasticsearch client");
        } catch (final Exception e) {
            logger.error("Error closing ElasticSearch client:", e);
        }
    }

    public TransportClient getObject() throws Exception {
        return client;
    }

    public Class<TransportClient> getObjectType() {
        return TransportClient.class;
    }

    public boolean isSingleton() {
        return false;
    }

    public void afterPropertiesSet() throws Exception {
        buildClient();
    }

    private void buildClient() throws Exception{
       client = new PreBuiltTransportClient(settings());
        Assert.hasText(clusterNodes, "[Assertion failed] clusterNodes settings missing.");
        for (String clusterNode : split(clusterNodes, COMMA)) {
            String hostName = substringBeforeLast(clusterNode, COLON);
            String port = substringAfterLast(clusterNode, COLON);
            Assert.hasText(hostName, "[Assertion failed] missing host name in 'clusterNodes'");
            Assert.hasText(port, "[Assertion failed] missing port in 'clusterNodes'");
            logger.info("adding transport node : " + clusterNode);
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port)));
        }
        client.connectedNodes();
    }

    private Settings settings(){

        return Settings.builder()
                        .put("cluster.name", clusterName).
                        put("client.transport.sniff", clientTransportSniff)
                        .put("client.transport.ignore_cluster_name", clientIgnoreClusterName)
                        .put("client.transport.ping_timeout", clientPingTimeout)
                        .put("client.transport.nodes_sampler_interval", clientNodesSamplerInterval)
                        .build();
    }
}
