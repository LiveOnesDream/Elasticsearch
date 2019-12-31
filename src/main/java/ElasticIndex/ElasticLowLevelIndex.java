package ElasticIndex;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class ElasticLowLevelIndex {


    public void createIndex(RestClient client) {
        Map<String, String> params = Collections.emptyMap();
        String settings = "{ " +
                "\"setting\":{" +
                "\"number_of_shards\":3," +
                "\"number_of_replicas\":1" +
                "}," +
                "\"name\":\"gaolujie yagao\"," +
                "\"desc\":\"meibai\"," +
                "\"price\":25," +
                "\"producer\":\"meibai\"" +
                "}";

        HttpEntity entity = new NStringEntity(settings, ContentType.APPLICATION_JSON);
        try {
            Response response = client.performRequest("GET", "/rest_index/rest_type", params, entity);

            RequestLine requestLine = response.getRequestLine();//关于执行请求的信息
            HttpHost host = response.getHost();//返回响应的主机
            Header[] headers = response.getHeaders();//返回响应头
            String header = response.getHeader("");//获取指定名称的响应头
            String responseBody = EntityUtils.toString(response.getEntity());//响应体包含在org.apache.http.HttpEntity对象中

            //返回状态码
            System.out.println(response.getStatusLine().getStatusCode());
            //打印数据
            System.out.println(EntityUtils.toString(response.getEntity()));

            //方式4：提供谓词，终节点，可选查询字符串参数，可选请求主体
            // 以及用于为每个请求尝试创建org.apache.http.nio.protocol.HttpAsyncResponseConsumer回调实例的可选工厂来发送请求。
            // 控制响应正文如何从客户端的非阻塞HTTP连接进行流式传输。
            // 如果未提供，则使用默认实现，将整个响应主体缓存在堆内存中，最大为100 MB。
            params = Collections.emptyMap();
            HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory =
                    new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024);
            response = client.performRequest("GET", "indeName/typeName", params, null, consumerFactory);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setMapping(RestClient client) throws IOException {

        String mapping = "{\n" +
                "\t\t\"mappings\": {\n" +
                "\t\t\t\"blog\": {\n" +
                "\t\t\t  \"properties\": {\n" +
                "\t\t\t\t\"title\": {\n" +
                "\t\t\t\t  \"type\": \"text\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t\"body\": {\n" +
                "\t\t\t\t  \"type\": \"text\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t\"tags\": {\n" +
                "\t\t\t\t  \"type\": \"keyword\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t\"published_on\": {\n" +
                "\t\t\t\t  \"type\": \"keyword\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t\"comments\": {\n" +
                "\t\t\t\t  \"type\": \"nested\",\n" +
                "\t\t\t\t  \"properties\": {\n" +
                "\t\t\t\t\t\"name\": {\n" +
                "\t\t\t\t\t  \"type\": \"text\"\n" +
                "\t\t\t\t\t},\n" +
                "\t\t\t\t\t\"comment\": {\n" +
                "\t\t\t\t\t  \"type\": \"text\"\n" +
                "\t\t\t\t\t},\n" +
                "\t\t\t\t\t\"age\": {\n" +
                "\t\t\t\t\t  \"type\": \"short\"\n" +
                "\t\t\t\t\t},\n" +
                "\t\t\t\t\t\"rating\": {\n" +
                "\t\t\t\t\t  \"type\": \"short\"\n" +
                "\t\t\t\t\t},\n" +
                "\t\t\t\t\t\"commented_on\": {\n" +
                "\t\t\t\t\t  \"type\": \"text\"\n" +
                "\t\t\t\t\t}\n" +
                "\t\t\t\t  }\n" +
                "\t\t\t\t}\n" +
                "\t\t\t  }\n" +
                "\t\t\t}\n" +
                "\t\t  }\n" +
                "\t\t}";

        HttpEntity entity = new NStringEntity(mapping, ContentType.APPLICATION_JSON);
        Response response = null;
        try {
            response = client.performRequest("PUT", "rest_mapping", Collections.emptyMap(), entity);
            System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void deleteIndex(RestClient client, String index) throws IOException {
        HttpEntity entity = new NStringEntity("", ContentType.APPLICATION_JSON);

        Response response = client.performRequest("DELETE", "/" + index + "/", Collections.emptyMap(), entity);
        System.out.println(EntityUtils.toString(response.getEntity()));
    }


    public void deleteByCondition(RestClient client) {
        String endPoint = "";

        HttpEntity entity = new NStringEntity(endPoint, ContentType.APPLICATION_JSON);

        try {
            Response response = client.performRequest("DELETE", endPoint, Collections.emptyMap(), entity);
            System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public RestClient getClient() {
        RestClient client = RestClient
                .builder(new HttpHost("hadoop5", 9200, "http"))
                .build();

        return client;
    }

    public void clientClose(RestClient client) {
        if (client != null) {
            try {
                client.close();
                System.out.println(client.getNodes() + "节点关闭");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
