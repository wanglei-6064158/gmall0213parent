package com.atguigu.gmall0213publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0213publisher.service.DauService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {
    @Autowired
    DauService dauService;

    @RequestMapping("/realtime-total")
    public String realtimeTotal(@RequestParam("date") String date)
    {
        List<Map<String,Object>> totalList = new ArrayList<>();
        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = dauService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);
        Map newMidMap = new HashMap();
        newMidMap.put("id","newMid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);
    }

    @RequestMapping("realtime-hour")
    public String realtimeHour(@RequestParam("date") String date,@RequestParam("id") String id)
    {
        Map<String,Map<String,Long>> resultMap= new HashMap<>();
        if("dau".equals(id))
        {
            Map<String, Long> dauHourCountToday = dauService.getDauHourCount(date);
            resultMap.put("today",dauHourCountToday);
            String yd = getYD(date);
            Map<String, Long> dauHourCountYd = dauService.getDauHourCount(yd);
            resultMap.put("yesterday",dauHourCountYd);
        }
        return JSON.toJSONString(resultMap);
    }

    private String getYD(String td)  {
        String yd = null;
        if(td != null && !td.equals(""))
        {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            try {
                Date tdDate = simpleDateFormat.parse(td);
                Date ydDate = DateUtils.addDays(tdDate, -1);
                yd = simpleDateFormat.format(ydDate);
            } catch (ParseException e) {
                e.printStackTrace();
                throw new RuntimeException("格式转换异常");
            }
        }
        return yd;
    }
}
