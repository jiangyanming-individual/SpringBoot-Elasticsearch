package com.jiang.springbootelasticsearch.config;

import com.alibaba.fastjson.support.config.FastJsonConfig;
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.nio.charset.Charset;
import java.util.List;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Bean
    FastJsonHttpMessageConverter getFastJsonHttpMessageConverter() {
        FastJsonHttpMessageConverter converter  = new FastJsonHttpMessageConverter();
        FastJsonConfig fastJsonConfig = new FastJsonConfig();

        //编码格式：
        fastJsonConfig.setCharset(Charset.forName("UTF-8"));
        //日期格式
        fastJsonConfig.setDateFormat("yyyy-MM-dd HH:mm:ss");
        converter.setFastJsonConfig(fastJsonConfig);
        converter.setDefaultCharset(Charset.forName("UTF-8"));
        return converter;
    }

    /**
     * 使用 FastJsonHttpMessageConverter 来替换 Spring MVC 默认的 HttpMessageConverter
     * 以提高 @RestController @ResponseBody @RequestBody 注解的 JSON序列化和反序列化速度。
     * @param converters
     */
    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        FastJsonHttpMessageConverter converter = new FastJsonHttpMessageConverter();
        FastJsonConfig jsonConfig = new FastJsonConfig();
        //设置编码
        jsonConfig.setCharset(Charset.forName("UTF-8"));
        jsonConfig.setDateFormat("yyyy-MM-dd HH:mm:ss");
        converter.setFastJsonConfig(jsonConfig);
        converter.setDefaultCharset(Charset.forName("Utf-8"));
        converters.add(converter);
    }

}
