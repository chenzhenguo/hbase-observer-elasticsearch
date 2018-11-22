package com.hbase.observer.v7;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.eminem.hbase.observer6.ESClient;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ESClientv7 {
    private static final Log log = LogFactory.getLog(ESClientv7.class);
    // ElasticSearch的集群名称
    public static String clusterName;
    // ElasticSearch的host
    public static String nodeHost;
    public static int nodePort;
    public static TransportClient client;

    /**
     * get Es config
     *
     * @return
     */
    public static String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : ESClientv7.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(null));
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
        return StringUtils.join(fields, ", ");
    }

    /**
     * init ES client
     */
    public static void initEsClient() throws UnknownHostException {
        if (client == null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
            log.info("开始 initESClient:"+sdf.format(new Date()));
            // 创建配置对象 myClusterName处为es集群名称
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", ESClient.clusterName).build();
            // 创建客户端 host1,host2处为es集群节点的ip地址
            client = TransportClient
                    .builder()
                    .settings(settings)
                    .build()
                    .addTransportAddress(
                            new InetSocketTransportAddress(InetAddress
                                    .getByName(ESClient.nodeHost), ESClient.nodePort));
            log.info("完成 initESClient:"+sdf.format(new Date()));
        }
    }

    /**
     * Close ES client
     */
    public static void closeEsClient() {
        try{
            client.close();
        }catch(Exception e){
            String fullStackTrace = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e);
            System.out.println("closeEsClient:" + fullStackTrace);
        }
    }
}