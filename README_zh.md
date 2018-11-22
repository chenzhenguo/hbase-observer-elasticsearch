# 使用Hbase协作器(Coprocessor)同步数据到ElasticSearch

最近项目中需要将Hbase中的数据同步到ElasticSearch中，需求就是只要往Hbase里面put或者delete数据，那么ES集群中，相应的索引下，也需要更新或者删除这条数据。本人使用了hbase-rirver插件，发现并没有那么好用，于是到网上找了一些资料，自己整理研究了一下，就自己写了一个同步数据的组件，基于Hbase的协作器，效果还不错，现在共享给大家，如果大家发现什么需要优化或者改正的地方，可以在我的csdn博客：[我的csdn博客地址](http://blog.csdn.net/fxsdbt520)上面私信我给我留言，代码托管在码云上[Hbase-Observer-ElasticSearch](https://git.oschina.net/eminem89/Hbase-Observer-ElasticSearch.git)。同时要感谢Gavin Zhang 2shou，我虽然不认识Gavin Zhang 2shou，[（2shou的同步数据博文）](http://guoze.me/2015/04/23/hbase-observer-sync-elasticsearch/)但是我是看了他写的代码以及博客之后，[（2shou的同步组件代码）](https://github.com/2shou/HBaseObserver.git)在他的基础之上对代码做了部分优化以及调整，来满足我本身的需求，所以在此表示感谢，希望我把我的代码开源出来，其他人看到之后也能激发你们的灵感，来写出更多更好更加实用的东西：

- **Hbase协作器(Coprocessor)**
- **编写组件**
- **部署组件**
- **验证组件**
- **总结**


-------------------

##Hbase协作器(Coprocessor)
> HBase 0.92版本后推出了Coprocessor — 协处理器，一个工作在Master/RegionServer中的框架，能运行用户的代码，从而灵活地完成分布式数据处理的任务。

- **HBase 支持两种类型的协处理器，Endpoint 和 Observer。Endpoint 协处理器类似传统数据库中的存储过程，客户端可以调用这些 Endpoint 协处理器执行一段 Server 端代码，并将 Server 端代码的结果返回给客户端进一步处理，最常见的用法就是进行聚集操作。如果没有协处理器，当用户需要找出一张表中的最大数据，即 max 聚合操作，就必须进行全表扫描，在客户端代码内遍历扫描结果，并执行求最大值的操作。这样的方法无法利用底层集群的并发能力，而将所有计算都集中到 Client 端统一执行，势必效率低下。利用 Coprocessor，用户可以将求最大值的代码部署到 HBase Server 端，HBase 将利用底层 cluster 的多个节点并发执行求最大值的操作。即在每个 Region 范围内执行求最大值的代码，将每个 Region 的最大值在 Region Server 端计算出，仅仅将该 max 值返回给客户端。在客户端进一步将多个 Region 的最大值进一步处理而找到其中的最大值。这样整体的执行效率就会提高很多。**
- **另外一种协处理器叫做 Observer Coprocessor，这种协处理器类似于传统数据库中的触发器，当发生某些事件的时候这类协处理器会被 Server 端调用。Observer Coprocessor 就是一些散布在 HBase Server 端代码中的 hook 钩子，在固定的事件发生时被调用。比如：put 操作之前有钩子函数 prePut，该函数在 put 操作执行前会被 Region Server 调用；在 put 操作之后则有 postPut 钩子函数。**
- **在实际的应用场景中，第二种Observer Coprocessor应用起来会比较多一点，因为第二种方式比较灵活，可以针对某张表进行绑定，假如hbase有十张表，我只想绑定其中的5张表，另外五张不需要处理，就不绑定即可，下面我要介绍的也是第二种方式。 **

## 编写组件

> 首先编写一个ESClient客户端，用于链接访问的ES集群代码。

```
package org.eminem.hbase.observer;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * ES Cleint class
 */
public class ESClient {

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
    public static Client client;

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
            ex.printStackTrace();
        }
        return StringUtils.join(fields, ", ");
    }

    /**
     * init ES client
     */
    public static void initEsClient() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", ESClient.clusterName).build();
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        ESClient.nodeHost, ESClient.nodePort));
    }

    /**
     * Close ES client
     */
    public static void closeEsClient() {
        client.close();
    }
}

```

> 然后编写一个Class类，继承BaseRegionObserver，并复写其中的start()、stop()、postPut()、postDelete()、四个方法。这四个方法其实很好理解，分别表示协作器开始、协作器结束、put事件触发并将数据存入hbase之后我们可以做一些事情，delete事件触发并将数据从hbase删除之后我们可以做一些事情。我们只要将初始化ES客户端的代码写在start中，在stop中关闭ES客户端以及定义好的Scheduled对象即可。两个触发事件分别bulk hbase中的数据到ES，就轻轻松松的搞定了。

```
package org.eminem.hbase.observer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Hbase Sync data to Es Class
 */
public class HbaseDataSyncEsObserver extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(HbaseDataSyncEsObserver.class);


    /**
     * read es config from params
     * @param env
     */
    private static void readConfiguration(CoprocessorEnvironment env) {
        Configuration conf = env.getConfiguration();
        ESClient.clusterName = conf.get("es_cluster");
        ESClient.nodeHost = conf.get("es_host");
        ESClient.nodePort = conf.getInt("es_port", -1);
        ESClient.indexName = conf.get("es_index");
        ESClient.typeName = conf.get("es_type");
    }

    /**
     *  start
     * @param e
     * @throws IOException
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        // read config
         readConfiguration(e);
         // init ES client
         ESClient.initEsClient();
        LOG.error("------observer init EsClient ------"+ESClient.getInfo());
    }

    /**
     * stop
     * @param e
     * @throws IOException
     */
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // close es client
       ESClient.closeEsClient();
       // shutdown time task
       ElasticSearchBulkOperator.shutdownScheduEx();
    }

    /**
     * Called after the client stores a value
     * after data put to hbase then prepare update builder to bulk  ES
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(put.getRow());
        try {
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            Map<String, Object> infoJson = new HashMap<String, Object>();
            Map<String, Object> json = new HashMap<String, Object>();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    json.put(key, value);
                }
            }
            // set hbase family to es
            infoJson.put("info", json);
            ElasticSearchBulkOperator.addUpdateBuilderToBulk(ESClient.client.prepareUpdate(ESClient.indexName, ESClient.typeName, indexId).setDocAsUpsert(true).setDoc(infoJson));
        } catch (Exception ex) {
            LOG.error("observer put  a doc, index [ " + ESClient.indexName + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());
        }
    }


    /**
     * Called after the client deletes a value.
     * after data delete from hbase then prepare delete builder to bulk  ES
     * @param e
     * @param delete
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(delete.getRow());
        try {
            ElasticSearchBulkOperator.addDeleteBuilderToBulk(ESClient.client.prepareDelete(ESClient.indexName, ESClient.typeName, indexId));
        } catch (Exception ex) {
            LOG.error(ex);
            LOG.error("observer delete  a doc, index [ " + ESClient.indexName + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());

        }
    }
}

```
![这里写图片描述](http://img.blog.csdn.net/20161226112006655?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvZnhzZGJ0NTIw/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)
**这段代码中info节点是根据我这边自身的需求加的，大家可以结合自身需求，去掉这个info节点，直接将hbase中的字段写入到ES中去。我们的需求需要把hbase的Family也要插入到ES中。**

> 最后就是比较关键的bulk ES代码，结合2shou的代码，我自己写的这部分代码，没有使用Timer，而是使用了ScheduledExecutorService，至于为什么不使用Timer，大家可以去百度上面搜索下这两个东东的区别，我在这里就不做过多的介绍了。在ElasticSearchBulkOperator这个类中，我使用ScheduledExecutorService周期性的执行一个任务，去判断缓冲池中，是否有需要bulk的数据，阀值是10000.每30秒执行一次，如果达到阀值，那么就会立即将缓冲池中的数据bulk到ES中，并清空缓冲池中的数据，等待下一次定时任务的执行。当然，初始化定时任务需要一个beeper响铃的线程，delay时间10秒。还有一个很重要的就是需要对bulk的过程进行加锁操作。

```
package org.eminem.hbase.observer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bulk hbase data to ElasticSearch Class
 */
public class ElasticSearchBulkOperator {

    private static final Log LOG = LogFactory.getLog(ElasticSearchBulkOperator.class);

    private static final int MAX_BULK_COUNT = 10000;

    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static final Lock commitLock = new ReentrantLock();

    private static ScheduledExecutorService scheduledExecutorService = null;

    static {
        // init es bulkRequestBuilder
        bulkRequestBuilder = ESClient.client.prepareBulk();
        bulkRequestBuilder.setRefresh(true);

        // init thread pool and set size 1
        scheduledExecutorService = Executors.newScheduledThreadPool(1);

        // create beeper thread( it will be sync data to ES cluster)
        // use a commitLock to protected bulk es as thread-save
        final Runnable beeper = new Runnable() {
            public void run() {
                commitLock.lock();
                try {
                    bulkRequest(0);
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                    LOG.error("Time Bulk " + ESClient.indexName + " index error : " + ex.getMessage());
                } finally {
                    commitLock.unlock();
                }
            }
        };

        // set time bulk task
        // set beeper thread(10 second to delay first execution , 30 second period between successive executions)
        scheduledExecutorService.scheduleAtFixedRate(beeper, 10, 30, TimeUnit.SECONDS);

    }

    /**
     * shutdown time task immediately
     */
    public static void shutdownScheduEx() {
        if (null != scheduledExecutorService && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    /**
     * bulk request when number of builders is grate then threshold
     *
     * @param threshold
     */
    private static void bulkRequest(int threshold) {
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
            if (!bulkItemResponse.hasFailures()) {
                bulkRequestBuilder = ESClient.client.prepareBulk();
            }
        }
    }

    /**
     * add update builder to bulk
     * use commitLock to protected bulk as thread-save
     * @param builder
     */
    public static void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(" update Bulk " + ESClient.indexName + " index error : " + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * add delete builder to bulk
     * use commitLock to protected bulk as thread-save
     *
     * @param builder
     */
    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(" delete Bulk " + ESClient.indexName + " index error : " + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }
}

```

>至此，代码已经全部完成了，接下来只需要我们打包部署即可。

### 部署组件

- **使用maven打包**
```
mvn clean package

```

- **使用shell命令上传到hdfs**

````
hadoop fs -put hbase-observer-elasticsearch-1.0-SNAPSHOT-zcestestrecord.jar /hbase_es
hadoop fs -chmod -R 777 /hbase_es
````

###验证组件

- **hbase shell**

```
create 'test_record','info'

disable 'test_record'

alter 'test_record', METHOD => 'table_att', 'coprocessor' => 'hdfs:///hbase_es/hbase-observer-elasticsearch-1.0-SNAPSHOT-zcestestrecord.jar|org.eminem.hbase.observer.HbaseDataSyncEsObserver|1001|es_cluster=zcits,es_type=zcestestrecord,es_index=zcestestrecord,es_port=9100,es_host=master'

enable 'test_record'

put 'test_record','test1','info:c1','value1'
deleteall 'test_record','test1'
```
> 绑定操作之前需要，在ES集群中建立好相应的索引以下是对绑定代码的解释:
> 把Java项目打包为jar包，上传到HDFS的特定路径
进入HBase Shell，disable你希望加载的表
通过alert 命令激活Observer
coprocessor对应的格式以|分隔，依次为：
- **jar包的HDFS路径**
- **Observer的主类**
- **优先级（一般不用改）**
- **参数（一般不用改）**
- **新安装的coprocessor会自动生成名称：coprocessor + $ + 序号（通过describe table_name可查看）**

- **以后对jar包内容做了调整，需要重新打包并绑定新jar包，再绑定之前需要做目标表做解绑操作，加入目标表之前绑定了同步组件的话，以下是解绑的命令**


- **hbase shell**

```
disable 'test_record'
alter 'test_record', METHOD => 'table_att_unset',NAME => 'coprocessor$1'
enable 'test_record'
desc 'test_record'
```


## 总结

- **绑定之后如果在执行的过程中有报错或者同步不过去，可以到hbase的从节点上的logs目录下，查看hbase-roor-regionserver-slave*.log文件。因为协作器是部署在regionserver上的，所以要到从节点上面去看日志，而不是master节点。 **

- **hbase-river插件之前下载了源代码看了下，hbase-river插件是周期性的scan整张表进行bulk操作，而我们这里自己写的这个组件呢，是基于hbase的触发事件来进行的，两者的效果和性能不言而喻，一个是全量的，一个是增量的，我们在实际的开发中，肯定是希望如果有数据更新了或者删除了，我们只要对着部分数据进行同步就行了，没有修改或者删除的数据，我们可以不用去理会。 **

- **Timer和 ScheduledExecutorService，在这里我选择了ScheduledExecutorService，2shou之前提到过部署插件有个坑，修改Java代码后，上传到HDFS的jar包文件必须和之前不一样，否则就算卸载掉原有的coprocessor再重新安装也不能生效，这个坑我也碰到了，就是因为没有复写stop方法，将定时任务停掉，线程一直会挂在那里，而且一旦报错将会导致hbase无法启动，必须要kill掉相应的线程。这个坑，坑了我一段时间，大家千万要注意，一定记得要复写stop方法，关闭之前打开的线程或者客户端，这样才是最好的方式。**
