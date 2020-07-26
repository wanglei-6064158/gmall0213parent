package com.atguigu.gmall0213.gmall0213logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate kafkaTemplate;


    @RequestMapping("/applog")
    public String applog(@RequestBody String json)
    {

        JSONObject jsonObject = JSON.parseObject(json);
        if(jsonObject.getString("start") != null && jsonObject.getString("start").length() > 0)
        {
            kafkaTemplate.send("GMALL_START",json);
        }else{
            kafkaTemplate.send("GMALL_EVENT",json);
//            kafkaTemplate.send("atguigu",json);
        }
        log.info(json);
        return "success";
    }
}
