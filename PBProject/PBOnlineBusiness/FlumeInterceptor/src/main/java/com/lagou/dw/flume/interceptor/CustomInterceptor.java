/**
 * @Author: april
 * @Date: 3/10/21 5:21 PM
 * @Project: PBOnlineBusiness
 * @Product: IntelliJ IDEA
 * @Package: com.lagou.dw.flume.interceptor
 */

package com.lagou.dw.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Test;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
//        1、获取 event 的 header
        Map<String, String> headers = event.getHeaders();
//        2、获取 event 的 body
        String eventBody = new String(event.getBody(), Charsets.UTF_8);
//        3、解析body获取json串
        //使用空格分割，无视空格的个数
        String[] bodyArr = eventBody.split("\\s+");
        try {
            String jsonStr = bodyArr[6];
            JSONObject jsonObject = JSON.parseObject(jsonStr);

            //        4、解析json串获取时间戳
            String timeStampStr = jsonObject.getJSONObject("app_active").getString("time");


            //        5、将时间戳转换为字符串 "yyyy-MM-dd"
            long timeStamp = Long.parseLong(timeStampStr);
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            Instant instant = Instant.ofEpochMilli(timeStamp);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            String date = dateTimeFormatter.format(localDateTime);

            //        6、将转换后的字符串放置header中
            headers.put("logtime", date);
            event.setHeaders(headers);

        } catch (Throwable e) {
//            e.printStackTrace();
            headers.put("logtime", "Unknown");
            event.setHeaders(headers);
        }

        //        7、返回event
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> eventList = new ArrayList<>();

        events.forEach(event -> {
            Event intercepted = intercept(event);
            if (intercepted != null) {
                eventList.add(intercepted);
            }
        });

        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {

            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

    @Test
    public void testJunit() {
        // test how it work
        String str = "11:56:08.703 [main] INFO  com.lagou.ecommerce.AppStart - {\"app_active\":{\"name\":\"app_active\",\"json\":{\"entry\":\"2\",\"action\":\"0\",\"error_code\":\"0\"},\"time\":1595329188129},\"attr\":{\"area\":\"泉州\",\"uid\":\"2F10092A9990\",\"app_v\":\"1.1.17\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1009990\",\"os_type\":\"0.18\",\"channel\":\"VS\",\"language\":\"chinese\",\"brand\":\"xiaomi-5\"}}";
        // new event
        SimpleEvent simpleEvent = new SimpleEvent();
        simpleEvent.setHeaders(new HashMap<>());
        simpleEvent.setBody(str.getBytes(Charsets.UTF_8));

        // interceptor
        CustomInterceptor customInterceptor = new CustomInterceptor();
        Event intercepted = customInterceptor.intercept(simpleEvent);
        // result
        System.out.println(intercepted.getHeaders());
    }
}
