package cn.bidlink.job.common.es;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Repository;

import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/4/27
 */
@Repository
public class ElasticClient implements InitializingBean{
    private Logger logger = LoggerFactory.getLogger(ElasticClient.class);
    private TransportClient transportClient;
    private Properties      properties;

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            initElasticSearchClient();
        } catch (Exception e) {
            logger.error("初始化elasticClient失败", e);
            throw new RuntimeException(e);
        }
    }

    private void initElasticSearchClient() throws Exception {
        properties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("elasticsearch.properties"));
        boolean searchGuardEnable = Boolean.parseBoolean(properties.getProperty("searchguard.enable"));
        if (searchGuardEnable) {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", properties.getProperty("cluster.name"))
                    .put("client.transport.sniff", true)
                    .put("path.home", ".")
                    .put("path.conf", properties.getProperty("path.conf"))
                    .put("searchguard.ssl.transport.enabled", true)
                    .put("searchguard.ssl.transport.keystore_filepath", properties.getProperty("searchguard.ssl.transport.keystore_filepath"))
                    .put("searchguard.ssl.transport.truststore_filepath", properties.getProperty("searchguard.ssl.transport.truststore_filepath"))
                    .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                    .put("searchguard.ssl.transport.enable_openssl_if_available", false)
                    .put("searchguard.ssl.transport.keystore_password", properties.getProperty("searchguard.ssl.transport.keystore_password"))
                    .put("searchguard.ssl.transport.truststore_password", properties.getProperty("searchguard.ssl.transport.truststore_password"))
                    .build();
            transportClient = TransportClient.builder()
                    .settings(settings)
                    .addPlugin(SearchGuardSSLPlugin.class)
                    .addPlugin(DeleteByQueryPlugin.class)
                    .build();
        } else {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", properties.getProperty("cluster.name"))
                    .put("client.transport.sniff", true)
                    .put("path.home", ".")
                    .put("path.conf", properties.getProperty("path.conf"))
                    .build();
            transportClient = TransportClient.builder()
                    .addPlugin(DeleteByQueryPlugin.class)
                    .settings(settings)
                    .build();
        }

        String[] hosts = properties.getProperty("cluster.host").split(",");
        for (String host : hosts) {
            if (host.contains(":")) {
                String ip = host.split(":")[0];
                int port = Integer.parseInt(host.split(":")[1]);
                transportClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(ip, port)));
            } else {
                transportClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(host, 9300)));
            }
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public TransportClient getTransportClient() {
        return transportClient;
    }
}
