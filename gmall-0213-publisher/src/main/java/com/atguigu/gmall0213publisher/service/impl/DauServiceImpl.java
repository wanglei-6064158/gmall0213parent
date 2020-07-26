package com.atguigu.gmall0213publisher.service.impl;

import com.atguigu.gmall0213publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    JestClient jestClient;
    @Override
    public Long getDauTotal(String date)  {

        String indexName = "gmall_dau_info_" + date + "-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        Long total = 0L;
        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw  new RuntimeException("es 查询异常");
        }
        return total;
    }

    @Override
    public Map<String, Long> getDauHourCount(String date) {
        String indexName = "gmall_dau_info_" + date + "-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsBuilder = AggregationBuilders.terms("groupby_hour").field("hr").size(24);
        searchSourceBuilder.aggregation(termsBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        Map<String,Long> resultMap = new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            if(null != searchResult && null != searchResult.getAggregations() && null != searchResult.getAggregations().getTermsAggregation("groupby_hour"))
            {
                List<TermsAggregation.Entry> groupby_hourBuckets = searchResult.getAggregations().getTermsAggregation("groupby_hour").getBuckets();
                for (TermsAggregation.Entry groupby_hourBucket : groupby_hourBuckets) {
                    resultMap.put(groupby_hourBucket.getKey(),groupby_hourBucket.getCount());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw  new RuntimeException("es 查询异常");
        }
        return resultMap;
    }
}
