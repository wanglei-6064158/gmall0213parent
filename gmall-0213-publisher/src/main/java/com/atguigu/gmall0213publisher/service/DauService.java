package com.atguigu.gmall0213publisher.service;

import java.io.IOException;
import java.util.Map;

public interface DauService {
    //某日日活总值
    public Long getDauTotal(String date) ;

    //某日日活分时值
    public Map<String,Long> getDauHourCount(String date);

}
