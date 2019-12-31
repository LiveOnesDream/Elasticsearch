package ElasticSearch;

import ElasticIndex.ElasticHighLevelIndex;

public class HighLeveAPP {
    public static void main(String[] args) throws Exception {
        String data_all = "data_all";
        String datas = "datas";
        String company = "company";
        String type = "doc";
        String tvs = "tvs";

       new ElasticHighLevelIndex().insertIndex("high_index");
//        new ElasticHighLevelIndex().createIndex1();
//        new ElasticHighLevelSearch().sdearch(data_all, type);
//        new ElasticHighLevelSearch().aggregation("company");
//        new ElasticHighLevelSearch().highLight("company");
//        new ElasticHighLevelSearch().suggestionSearch(company);
//        new ElasticHighLevelSearch().completionSuggester(company);
//        new ElasticHighLevelSearch().searchScroll(company);
//        new ElasticHighLevelSearch().geographicSearch("my_index");
//        new ElasticHighLevelSearch().geographicSearch1("hotel_app");
//        new ElasticHighLevelSearch().geographicSearch2("hotel_app");
//        new ElasticHighLevelSearch().aggregation("tvs");
//        new ElasticHighLevelSearch().secondaryGroup(tvs);
//         new ElasticHighLevelSearch().timeQuanyum("tvs");
    }
}
