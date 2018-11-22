package org.eminem.hbase.observer6;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * ES Cleint class
 */
public class ESClient {
    private static final Log LOG = LogFactory.getLog(ESClient.class);

    // ElasticSearch的集群名称
    public static String clusterName;
    // ElasticSearch的host
    public static String nodeHost;
    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
    public static int nodePort;
    // ElasticSearch的索引名称
    public static String indexName;
    // ElasticSearch的类型名称
    public static String typeName;
    // ElasticSearch Client
    public static TransportClient  client;

    /**
     * get Es config
     *
     * @return
     */
    public static String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : ESClient.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(null));
            }
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
            ex.printStackTrace();
        }
        return    StringUtils.join(",",fields);

    }

    /**
     * init ES client
     */
    public static void initEsClient() {

        try {
            // 创建配置对象 myClusterName处为es集群名称
            Settings settings = Settings.builder()
                    .put("cluster.name", ESClient.clusterName).build();
            // 创建客户端 host1,host2处为es集群节点的ip地址
             client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(ESClient.nodeHost),  ESClient.nodePort));

        } catch (UnknownHostException e) {
            LOG.error(e);

            e.printStackTrace();
        }
    }

    /**
     * Close ES client
     */
    public static void closeEsClient() {

        try{
            LOG.info("-----------Close ES client--------------");
            client.close();
        }catch(Exception e){
            String fullStackTrace = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e);
            System.out.println("closeEsClient:" + fullStackTrace);
        }
    }


    public static void main(String[] args) {

        try {
            // 创建配置对象 myClusterName处为es集群名称
            Settings settings = Settings.builder()
                    .put("cluster.name", "myClusterName").put("client.transport.sniff", true).build();
            // 创建客户端 host1,host2处为es集群节点的ip地址
            TransportClient   client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.1.182"),  9300));
            client.close();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        System.out.println("conntect success");

    }
}
