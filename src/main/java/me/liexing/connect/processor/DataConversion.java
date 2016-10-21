package me.liexing.connect.processor;

import java.util.List;
import java.util.Map;

/**
 * Created by jinsheng on 16/9/19.
 */
public interface DataConversion {
    List<Map> convert(Object source);
}
