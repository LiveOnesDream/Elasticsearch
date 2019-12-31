package ElasticIndex;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

//https://www.liangzl.com/get-article-detail-122769.html 参考
public class ElasticHighLevelIndex {
    private RestHighLevelClient client = getClient();

    public boolean indexExists(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        request.local(false);
        request.humanReadable(true);

        boolean exists = client.indices().exists(request);
        System.out.println("索引存在？" + exists);
        return exists;
    }

    /**
     * map 形式创建 mapping
     *
     * @param index
     * @throws IOException
     */

    public void createIndex(String index) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
                .put("analysis.analyzer.default.tokenizer", "ik_smart")// 默认分词器
        );

        Map properties = new HashMap();
        properties.put("name", new HashMap() {
            {
                put("type", "text");
            }
        });

        properties.put("age", new HashMap() {
            {
                put("type", "double");
            }
        });
        properties.put("sex", new HashMap() {
            {
                put("type", "double");
            }
        });
        properties.put("address", new HashMap() {
            {
                put("type", "text");
            }
        });

        Map jsonMap = new HashMap();
        Map mapping = new HashMap();
        mapping.put("properties", properties);
        jsonMap.put("doc", mapping);
        request.mapping("doc", jsonMap);

        request.alias(new Alias("bieming")); //别名
        //发送请求(同步)
        CreateIndexResponse indexResponse = client.indices().create(request);
        boolean acknowledged = indexResponse.isAcknowledged();
        boolean shardsAcknowledged = indexResponse.isShardsAcknowledged();
        System.out.println("acknowledged:" + acknowledged);
        System.out.println("shardsAcknowledged:" + shardsAcknowledged);

        //异步请求
        /*
        ActionListener<CreateIndexResponse> actionListener = new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                boolean acknowledged1 = indexResponse.isAcknowledged();
                boolean shardsAcknowledged1 = indexResponse.isShardsAcknowledged();
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("创建索引成功：" + e.getMessage());
            }
        };
        client.indices().createAsync(request,actionListener);
         */
    }

    /**
     * XContentBuilder  方式创建mapping
     *
     * @throws IOException
     */
    public void createIndex1() throws IOException {
        IndexRequest request = new IndexRequest();

        try {
            XContentBuilder builder = JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("mappings")
                    .startObject("doc")
                    .startObject("properties")

                    .startObject("title").field("type", "text").endObject()
                    .startObject("content").field("type", "text").endObject()
                    .startObject("uniqueId").field("type", "keyword").endObject()
                    .startObject("created").field("type", "text").endObject()

                    .endObject()
                    .endObject()
                    .endObject()

                    .startObject("settings")
                    .field("number_of_shards", 3)
                    .field("number_of_replicas", 1)
                    .endObject()

                    .endObject();

            request.source(builder);
            //生成josn字符串
            String source = request.source().utf8ToString();
            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
            //使用低版本的client
            RestClient client = RestClient.builder(new HttpHost("hadoop5", 9200, "http")).build();
            Response response = client.performRequest("put", "/demo", Collections.emptyMap(), entity);
            System.out.println(response.getStatusLine());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }

    }

    /**
     * index 存在 新增
     *
     * @param index
     * @throws IOException
     */

    public void insertIndex(String index) throws IOException {

        if (indexExists(index)) {
            indexDelete(index);
            createIndex(index);
        }
        IndexRequest request = new IndexRequest(index, "doc");

        Map<String, Object> map = new HashMap<>();
        map.put("name", "zhangsan");
        map.put("age", 21.5);
        map.put("sex", 0);
        map.put("address", "健康的恢复");

        request.source(map);

//        其他的一些可选设置
//        request.routing("routing");  //设置routing值
//        request.timeout(TimeValue.timeValueSeconds(1));  //设置主分片等待时长
//        request.setRefreshPolicy("wait_for");  //设置重刷新策略
//        request.version(2);  //设置版本号
//        request.opType(DocWriteRequest.OpType.CREATE);  //操作类别

        //发送请求
        IndexResponse response = null;
        try {
            //同步方式
            response = client.index(request);
        } catch (ElasticsearchException e) {
            // 捕获，并处理异常
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("文档冲突，处理冲突逻辑代码" + e.getDetailedMessage());
            }
            System.out.println("索引异常");
        }

        if (response != null) {
            String indexed = response.getIndex();
            String type = response.getType();
            String id = response.getId();
            long version = response.getVersion();

            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("新增文档成功");
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("修改文档");
            }

            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    System.out.println("副本失败原因:" + reason);
                }
            }
        }

        clientClose();
    }


    public void indexDelete(String index) {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        try {
            AcknowledgedResponse delete = client.indices().delete(request);
            boolean acknowledged = delete.isAcknowledged();
            System.out.println("索引删除：" + acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RestHighLevelClient getClient() {
        RestHighLevelClient highLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("wbbigdata00", 9200, "http")
                )
        );
        return highLevelClient;
    }

    public void clientClose() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
