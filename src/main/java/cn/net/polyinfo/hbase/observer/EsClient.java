package cn.net.polyinfo.hbase.observer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ES Cleint class
 */

public class EsClient {

    // ElasticSearch的集群名称
    private String clusterName;
    // ElasticSearch的host
    private String[] nodeHost;
    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP） 默认9300 es版本2.4
    private int nodePort;

    private TransportClient client = null;

    // ElasticSearch的索引名称
    public  String indexName;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    // ElasticSearch的类型名称
    public  String typeName;

    public String[] getNodeHost() {
        return nodeHost;
    }

    public void setNodeHost(String[] nodeHost) {
        this.nodeHost = nodeHost;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    private  final Log LOG = LogFactory.getLog(EsClient.class);

    /**
     * get Es config
     *
     * @return
     */
    public EsClient(String clusterName, String nodeHost, int nodePort) {
        this.clusterName = clusterName;
        this.nodeHost = nodeHost.split("-");
        this.nodePort = nodePort;
        this.client = initEsClient();

    }

    public EsClient() {

    }


    public String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : EsClient.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(this));
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
        return StringUtils.join( ", ",fields);
    }

    public String getOneNodeHost() {
        if (this.nodeHost == null || this.nodeHost.length == 0) {
            return "";
        }
        Random rand = new Random();
        return nodeHost[rand.nextInt(this.nodeHost.length)];

    }

    /**
     * init ES client
     */
    public TransportClient initEsClient() {
        LOG.info("---------- Init ES Client " + this.clusterName + " -----------");
        TransportClient client = null;
        Settings settings = Settings.builder().put("cluster.name", this.clusterName).put("client.transport.sniff", true).build();

        try {
            client = TransportClient
                    .builder()
                    .settings(settings)
                    .build()
                    .addTransportAddress(
                            new InetSocketTransportAddress(InetAddress
                                    .getByName(this.getOneNodeHost()), this.nodePort));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    public void repeatInitEsClient() {
        this.client = initEsClient();
    }

    /**
     * @return the clusterName
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * @param clusterName the clusterName to set
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }


    /**
     * @return the nodePort
     */
    public int getNodePort() {
        return nodePort;
    }

    /**
     * @param nodePort the nodePort to set
     */
    public void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }

    /**
     * @return the client
     */
    public TransportClient getClient() {
        return client;
    }

    /**
     * @param client the client to set
     */
    public void setClient(TransportClient client) {
        this.client = client;
    }

    /**
     * Close ES client
     */
    public  void closeEsClient() {
        LOG.info("-----------Close ES client--------------");

        client.close();
    }

}