package org.eminem.hbase.observer6;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;

import java.util.List;
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
     //   bulkRequestBuilder.setRefresh(true);


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
    private static synchronized void bulkRequest(int threshold) {
        int count = bulkRequestBuilder.numberOfActions();
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            try {
                LOG.info("Bulk Request Run " + ", the row count is: " + count);
                BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
                if (bulkItemResponse.hasFailures()) {
                    LOG.error("------------- Begin: Error Response Items of Bulk Requests to ES ------------");
                    LOG.error(bulkItemResponse.buildFailureMessage());
                    LOG.error("------------- End: Error Response Items of Bulk Requests to ES ------------");
                }
                bulkRequestBuilder = ESClient.client.prepareBulk();
            } catch (Exception e) {// two cause: 1. transport client is closed 2. None of the configured nodes are available
                LOG.error(" Bulk Request " + " index error : " + e.getMessage());
                LOG.error("Reconnect the ES server...");
                List<DocWriteRequest<?>> requests = bulkRequestBuilder.request().requests();
                ESClient.client.close();
                ESClient.initEsClient();
                bulkRequestBuilder = ESClient.client.prepareBulk();
                bulkRequestBuilder.request().add(requests);
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
