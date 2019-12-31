package ElasticSearch;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

//https://www.liangzl.com/get-article-detail-122769.html
public class ElasticHighLevelSearch {
    private RestHighLevelClient client = getClient();

    public void getDocument(String index) throws IOException {
        GetRequest request = new GetRequest(index, "doc", "event_e201904160940308059");
        request.fetchSourceContext(new FetchSourceContext(true)); //是否获取_source字段

        String[] includes = new String[]{"words", "subject"};
        String[] excludes = Strings.EMPTY_ARRAY;

        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);

        //取某一个字段
//        GetResponse getResponse = client.get(request);
//        String field = getResponse.getField("").getValue();

        GetResponse getResponse = null;
        try {
            // 同步请求
            getResponse = client.get(request);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("没有找到该id的文档");
            }
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("获取时版本冲突了，请在此写冲突处理逻辑！");
            }
            System.out.println("获取文档异常:" + e);
        }


        //4、处理响应
        if (getResponse != null) {
            String indexName = getResponse.getIndex();
            String type = getResponse.getType();
            String id = getResponse.getId();
            if (getResponse.isExists()) { // 文档存在
                long version = getResponse.getVersion();
                String sourceAsString = getResponse.getSourceAsString(); //结果取成 String
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();  // 结果取成Map
                byte[] sourceAsBytes = getResponse.getSourceAsBytes();    //结果取成字节数组

                System.out.println("index:" + indexName + "  type:" + type + "  id:" + id);
                System.out.println(sourceAsString);

            } else {
                System.out.println("没有找到该id的文档1111");
            }
        }
        clientClose();
    }

    public void search(String index, String type) throws IOException {
        SearchRequest request = new SearchRequest(index);
        request.types(type);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.queryStringQuery("dataSource:微博"));
        sourceBuilder.query(QueryBuilders.termQuery("province", "北京"));

        sourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("", "")));
        sourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("", "")));
        sourceBuilder.query(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("", "")));

        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        //是否返回_source字段
        sourceBuilder.fetchSource(true);
//        指定排序
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.ASC));
//        sourceBuilder.sort(new FieldSortBuilder("insertTime").order(SortOrder.ASC));

//         设置返回 profile
        sourceBuilder.profile(true);
        request.source(sourceBuilder);
        request.routing("event_e201904181832018580");

        SearchResponse searchResponse = client.search(request);
        //处理响应 搜索结果状态信息
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminateEarly = searchResponse.isTerminatedEarly();
        boolean timeOut = searchResponse.isTimedOut();

        //分片搜索情况
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }
        //处理搜索命中文档结果
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {

            // do something with the SearchHit
            String indexx = hit.getIndex();
            String typee = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();

            //取_source字段值
            String sourceAsString = hit.getSourceAsString(); //取成json串
            Map<String, Object> sourceAsMap = hit.getSourceAsMap(); // 取成map对象
            //从map中取字段值
				/*
				String documentTitle = (String) sourceAsMap.get("title");
				List<Object> users = (List<Object>) sourceAsMap.get("user");
				Map<String, Object> innerObject = (Map<String, Object>) sourceAsMap.get("innerObject");
				*/
            System.out.println("index:" + indexx + "  type:" + typee + "  id:" + id);
            System.out.println(sourceAsString);

        }
        clientClose();
    }

    /**
     * 游标查询
     *
     * @param index
     * @throws IOException
     */

    public void searchScroll(String index) throws IOException {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.scroll(scroll);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("country", "china"));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        String scrollID = null;
        if (RestStatus.OK.equals(searchResponse.status())) {
            scrollID = searchResponse.getScrollId();
            System.out.println(scrollID);
            SearchHit[] hits = searchResponse.getHits().getHits();
            while (hits != null && hits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollID);
                scrollRequest.scroll(scroll);
                searchResponse = client.searchScroll(scrollRequest);
                scrollID = searchResponse.getScrollId();
//                System.out.println(scrollID);
                hits = searchResponse.getHits().getHits();
                for (SearchHit hit : hits) {
                    System.out.println(hit.getSourceAsString());
                }

            }
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollID);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
        boolean succeeded = clearScrollResponse.isSucceeded();

        clientClose();
    }

    /**
     * 地理位置搜索
     *
     * @param index
     * @throws IOException
     */

    public void geographicSearch(String index) throws IOException {
        SearchRequest request = new SearchRequest(index);
        GeoPoint top_left = new GeoPoint(42, -72);
        GeoPoint bottom_right = new GeoPoint(40., -74);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(
                new GeoBoundingBoxQueryBuilder("location").setCorners(top_left, bottom_right));

        request.source(sourceBuilder);
        SearchResponse searchResponse = client.search(request);
        if (RestStatus.OK.equals(searchResponse.status())) {
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit str : hits) {
                System.out.println(str.getSourceAsString());
            }
        }
        clientClose();
    }

    /**
     * 地理位置搜索2
     *
     * @param index
     * @throws IOException
     */
    public void geographicSearch1(String index) throws IOException {
        SearchRequest request = new SearchRequest(index);
        GeoPoint top_left = new GeoPoint(40.73, -74.1);
        GeoPoint bottom_right = new GeoPoint(40.01, -71.12);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).
                        filter(new GeoBoundingBoxQueryBuilder("pin.location").setCorners(top_left, bottom_right)));
        request.source(sourceBuilder);
        SearchResponse searchResponse = client.search(request);
        if (RestStatus.OK.equals(searchResponse.status())) {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        }
        clientClose();
    }

    /**
     * 多边形地理搜索
     *
     * @param index
     * @throws IOException
     */

    public void geographicSearch2(String index) throws IOException {
        GeoPoint top_left = new GeoPoint(40.73, -74.1);
        GeoPoint bottom_right = new GeoPoint(40.01, -71.12);
        GeoPoint bottom_right1 = new GeoPoint(50.56, -90.58);

        List<GeoPoint> pointList = new LinkedList<GeoPoint>();
        pointList.add(top_left);
        pointList.add(bottom_right);
        pointList.add(bottom_right1);

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).
                        filter(QueryBuilders.geoPolygonQuery("pin.location", pointList)));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        if (RestStatus.OK.equals(searchResponse.status())) {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        }
        clientClose();
    }

    /**
     * 聚合
     *
     * @param index
     * @throws IOException
     */
    public void aggregation(String index) throws IOException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0);

        AggregationBuilder aggregation = AggregationBuilders
                .terms("group_by_color").field("color")
                .subAggregation(AggregationBuilders.max("max_price").field("price"))
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"));

        sourceBuilder.aggregation(aggregation);

        request.source(sourceBuilder);

        SearchResponse searchResponse = client.search(request);
        //  处理响应  搜索结果状态信息
        if (RestStatus.OK.equals(searchResponse.status())) {
            // 获取聚合结果
            Aggregations aggregations = searchResponse.getAggregations();
            Terms byAgeAggregation = aggregations.get("group_by_color");

            for (Terms.Bucket color_buk : byAgeAggregation.getBuckets()) {

                System.out.println("key: " + color_buk.getKeyAsString());
                System.out.println("docCount: " + color_buk.getDocCount());

                //取子聚合
                Avg averageBalance = color_buk.getAggregations().get("avg_price");
                System.out.println("avg_price: " + averageBalance.getValue());
            }
        }
        //直接用key 来去分组
                /*Bucket elasticBucket = byCompanyAggregation.getBucketByKey("24");
                Avg averageAge = elasticBucket.getAggregations().get("average_age");
                double avg = averageAge.getValue();*/
        clientClose();
    }

    /**
     * 二级分组(下钻)+聚合
     *
     * @param index
     * @throws IOException
     */
    public void secondaryGroup(String index) throws IOException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0);

        TermsAggregationBuilder groupColor = AggregationBuilders.terms("group_by_color").field("color");
        TermsAggregationBuilder groupBrandAvgPrice = AggregationBuilders.terms("group_by_brand").field("brand")
                .order(BucketOrder.aggregation("avg_price", false))
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"));

        groupColor.subAggregation(groupBrandAvgPrice);

        sourceBuilder.aggregation(groupColor);
        request.source(sourceBuilder);

        SearchResponse response = client.search(request);

        if (RestStatus.OK.equals(response.status())) {
            Terms terms = response.getAggregations().get("group_by_color");
            for (Terms.Bucket color_bucket : terms.getBuckets()) {
                //一级分组内容
                Terms color_term = color_bucket.getAggregations().get("group_by_brand");
                System.out.println("color key:    " + color_bucket.getKeyAsString());
                System.out.println("color docCount:   " + color_bucket.getDocCount());

                for (Terms.Bucket brand_bucket : color_term.getBuckets()) {
                    //二级分组内容
                    System.out.println("brand key:    " + brand_bucket.getKeyAsString());
                    System.out.println("brand docCount:   " + brand_bucket.getDocCount());

                    Avg averageBalance = brand_bucket.getAggregations().get("avg_price");
                    System.out.println("avg_price: " + averageBalance.getValue());
                }

            }
        }
        clientClose();
    }

    /**
     * 按照时间段查询 品牌去重
     *
     * @param index
     * @throws IOException
     */
    public void timeQuanyum(String index) throws IOException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0);

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = AggregationBuilders.dateHistogram("months")
                .field("sold_date").dateHistogramInterval(DateHistogramInterval.MONTH).format("yyyy-MM-dd")
                .subAggregation(AggregationBuilders.cardinality("distinct_colors").field("brand"));

        sourceBuilder.aggregation(dateHistogramAggregationBuilder);
        request.source(sourceBuilder);

        SearchResponse response = client.search(request);
        if (RestStatus.OK.equals(response.status())) {
            Map<String, Aggregation> results = response.getAggregations().asMap();
            Histogram histogram = (Histogram) results.get("months");
            for (Histogram.Bucket soluDate_bucket : histogram.getBuckets()) {
                System.out.println("key_as_string:  " + soluDate_bucket.getKeyAsString());
                System.out.println("key:    " + soluDate_bucket.getKey());
                System.out.println("doc_count:  " + soluDate_bucket.getDocCount());

                Cardinality cardinality = soluDate_bucket.getAggregations().get("distinct_colors");
                System.out.println("distinct_colors_value:  " + cardinality.getValue());
            }
        }
        clientClose();
    }

    /**
     * 高亮设置
     *
     * @param index
     * @throws IOException
     */
    public void highLight(String index) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("country", "china"))
                .from(0)
                .size(10);

        HighlightBuilder highlightBuilder = new HighlightBuilder();

        HighlightBuilder.Field hiField = new HighlightBuilder.Field("country").preTags("<strong>").postTags("</strong>");//标签高亮
        HighlightBuilder.Field hiField1 = new HighlightBuilder.Field("country").fragmentSize(150).numOfFragments(3).noMatchSize(150);//片段高亮
        HighlightBuilder.Field hiField2 = new HighlightBuilder.Field("country").highlighterType("plain");

        highlightBuilder.field(hiField).requireFieldMatch(false);

        sourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);

        //处理响应
        if (RestStatus.OK.equals(searchResponse.status())) {
            SearchHits hits = searchResponse.getHits();
            long totalHits = hits.getTotalHits();

            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                String indexName = hit.getIndex();
                String typeName = hit.getType();
                String id = hit.getId();
                float score = hit.getScore();
//                System.out.println("indexName: " + indexName + "  typeName: " + typeName + "  id: " + id + "  score: " + score);
                //获取_source 字段
                String sourceAsString = hit.getSourceAsString();
//                System.out.println(sourceAsString);
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();

                //获取高亮结果
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlight = highlightFields.get("country");
                if (highlight != null) {
                    Text[] fragments = highlight.getFragments();
                    if (fragments != null) {
                        String fragmentString = fragments[0].string();
                        System.out.println("country: " + fragmentString);
                    }
                }
            }
        }
        clientClose();
    }

    /**
     * 建议查询
     *
     * @param index
     */
    public void suggestionSearch(String index) throws Exception {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);

        // 做查询建议 词项建议
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("position").text("junior finance");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_position", termSuggestionBuilder);
        sourceBuilder.suggest(suggestBuilder);

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        //搜索结果状态信息
        if (RestStatus.OK.equals(searchResponse.status())) {
            // 获取建议结果
            Suggest suggest = searchResponse.getSuggest();
            TermSuggestion termSuggestion = suggest.getSuggestion("suggest_position");
            for (TermSuggestion.Entry entry : termSuggestion.getEntries()) {
                System.out.println("text: " + entry.getText().string());
                for (TermSuggestion.Entry.Option option : entry) {
                    String suggestText = option.getText().string();
                    System.out.println("suggest option : " + suggestText);
                }
            }
        }
        clientClose();
    }

    /**
     * 自动补全
     *
     * @param index
     * @throws IOException
     */

    public void completionSuggester(String index) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);

        SuggestionBuilder suggestionBuilder = SuggestBuilders.completionSuggestion("position").text("technique m").skipDuplicates(true);

        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("song-suggest", suggestionBuilder);

        sourceBuilder.suggest(suggestBuilder);

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        if (RestStatus.OK.equals(searchResponse.status())) {
            Suggest suggest = searchResponse.getSuggest();
            CompletionSuggestion tersuggestion = suggest.getSuggestion("song-suggest");
            for (CompletionSuggestion.Entry entry : tersuggestion.getEntries()) {
                System.out.println("text  :" + entry.getText().string());
                for (CompletionSuggestion.Entry.Option option : entry) {
                    String suggesText = option.getText().string();
                    System.out.println("suggest option ：" + suggesText);
                }
            }
        }
        clientClose();
    }

    public void bulkProcessor(String index, String type) {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

            }
        };
        //在这里调用build()方法构造bulkProcessor,在底层实际上是用了bulk的异步操作
        BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener)
                .setBulkActions(1000) // 1000条数据请求执行一次bulk
                .setBulkSize(new ByteSizeValue(5L, ByteSizeUnit.MB))   // 5mb的数据刷新一次bulk
                .setConcurrentRequests(0) // 并发请求数量, 0不并发, 1并发允许执行
                .setFlushInterval(TimeValue.timeValueSeconds(1L)) // 固定1s必须刷新一次
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 5)) // 重试5次，间隔1s
                .build();

        try {
            bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.out.println("Failed to close bulkProcessor" + e);
        }
        System.out.println("bulkProcessor closed!");
    }


    public RestHighLevelClient getClient() {
        RestHighLevelClient highLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("iZwz9hg8lhwat1ebtagp8gZ", 9200, "http"),
//                        new HttpHost("iZwz9hg8lhwat1ebtagp8fZ", 9200, "http")
                        new HttpHost("iZwz9h8uzjzogp0kn38ruzZ", 9200, "http")
                )
                        .setMaxRetryTimeoutMillis(100000)
                        .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                            @Override
                            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                                return builder.setSocketTimeout(100000);
                            }
                        })
                        .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS)

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
