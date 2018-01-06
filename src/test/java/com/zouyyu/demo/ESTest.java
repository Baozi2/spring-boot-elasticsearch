package com.zouyyu.demo;

import org.elasticsearch.action.search.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

/**
 * Unit test for simple ESApplication.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ESTest {


    @Autowired
    private TransportClient client;

    @Value("test01")
    private Resource test01;

    @Value("test02")
    private Resource test02;

    @Value("test03")
    private Resource test03;

    @Test//9859ms
    public  void  getFrom() throws Exception {

        //Result window is too large, from + size must be less than or equal to: [10000] but was [10500]
        int total = 9500;
        final int STEP = 500;
        QueryBuilder qb = QueryBuilders.matchQuery("color", "Pink");

        Path path = Paths.get(this.test01.getURI());
        long start = new Date().getTime();
        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.test02.getFile())))) {
            for (int i = 0; i <= total ; ){

                SearchResponse searchResponse = client
                    .prepareSearch()
                    .setIndices("index")
                    .setFrom(i)
                    .setSize(STEP)
                    .setPostFilter(qb).get();

                SearchHits searchHits = searchResponse.getHits();
                for (SearchHit hit :searchHits){

                        writer.write(hit.getSource().toString());

                }
                i += 500;

            }
        } catch (Exception e){
            System.out.println(e);
        }
        long end = new Date().getTime();

        System.out.println("from-size: " + (end-start));



    }

    @Test
    public void testScroll(){

        QueryBuilder qb = QueryBuilders.matchQuery("color.origin", "Pink");


       long start = new Date().getTime();

       SearchResponse response =  client.prepareSearch("index")
              .setScroll(new TimeValue(200000))
              .setQuery(qb)
              .setSize(500)
              .get();

       try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.test02.getFile())))) {
           for(int i = 0; i <= 10000; ){
               SearchResponse sr = client.prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueMinutes(20)).execute().actionGet();
               for (SearchHit hit: sr.getHits()){

                   writer.write(hit.getSource().toString());
               }
               i += sr.getHits().getHits().length;
           }
       }catch (Exception e){
       }

        long end = new Date().getTime();

        System.out.println("scroll: " + (end-start));

    }
    @Test
    public void testScrollScan(){


        QueryBuilder qb = QueryBuilders.matchQuery("color", "Pink");


        long start = new Date().getTime();

        SearchResponse response =  client.prepareSearch("index")
            .setScroll(new TimeValue(200000))
            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
            .setQuery(qb)
            .setSize(500)
            .get();

        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.test03.getFile())))) {
            for(int i = 0; i <= 10000; ){
                SearchResponse sr = client.prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueMinutes(20)).execute().actionGet();
                for (SearchHit hit: sr.getHits()){

                    writer.write(hit.getSource().toString());
                }
                i += sr.getHits().getHits().length;
            }
        }catch (Exception e){
        }

        long end = new Date().getTime();

        System.out.println("scroll-scan: " + (end-start));

    }
}

