package io.github.dflib.storage.jdbc;


import org.apache.commons.text.StringSubstitutor;

import java.util.Map;

public class TemplateUtil {

    public static String generateTemplate(String ftl, Map<String, Object> params) {
        StringSubstitutor sub = new StringSubstitutor(params);
        return  sub.replace(ftl);
    }


    //

}
