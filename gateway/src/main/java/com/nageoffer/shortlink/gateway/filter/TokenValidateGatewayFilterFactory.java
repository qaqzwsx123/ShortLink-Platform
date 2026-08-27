package com.nageoffer.shortlink.gateway.filter;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.nageoffer.shortlink.gateway.config.Config;          // 自定义配置类，持有白名单列表
import com.nageoffer.shortlink.gateway.dto.GatewayErrorResult; // 统一错误响应DTO
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * SpringCloud Gateway Token 校验过滤器工厂
 * 
 * 作用：对进入网关的请求进行登录态校验（基于 Redis 存储的 Token），
 * 校验通过后将用户信息（userId、realName）添加到请求头中，传递给下游微服务。
 * 支持白名单路径跳过校验。
 * 
 */
@Component
public class TokenValidateGatewayFilterFactory extends AbstractGatewayFilterFactory<Config> {

    // Redis 操作模板，用于查询用户登录 Token
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 构造器，Spring 自动注入 StringRedisTemplate
     */
    public TokenValidateGatewayFilterFactory(StringRedisTemplate stringRedisTemplate) {
        super(Config.class);  // 指定配置类为 Config，对应 yaml 中的 args 参数，通过反射进行传参
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 核心方法：创建 GatewayFilter 实例
     * @param config 从配置文件传入的过滤器参数（如 whitePathList）
     * @return 返回一个 GatewayFilter 函数式接口实现，定义过滤逻辑
     */
    @Override
    public GatewayFilter apply(Config config) {
        // 返回一个 lambda 表达式实现的 GatewayFilter
        return (exchange, chain) -> {
            // 1. 获取当前请求对象和请求路径、方法
            ServerHttpRequest request = exchange.getRequest();
            String requestPath = request.getPath().toString();
            String requestMethod = request.getMethod().name();

            // 2. 判断当前请求是否在白名单中（包括配置的白名单 + 硬编码的特殊注册接口）
            if (!isPathInWhiteList(requestPath, requestMethod, config.getWhitePathList())) {
                // ----- 非白名单，需要校验 Token -----

                // 从请求头中获取用户名和 Token（前端登录后应放在 header 中）
                String username = request.getHeaders().getFirst("username");
                String token = request.getHeaders().getFirst("token");

                Object userInfo;  // 用于存放从 Redis 查到的用户信息

                // 3. 校验用户名、Token 非空，并在 Redis 中查询是否存在该 Token
                //    Redis Key 格式：short-link:login:{username}，是一个 Hash 结构，field 为 token，value 为用户信息 JSON
                if (StringUtils.hasText(username) 
                        && StringUtils.hasText(token) 
                        && (userInfo = stringRedisTemplate.opsForHash().get("short-link:login:" + username, token)) != null) {

                    // ----- Token 有效，解析用户信息并传递给下游 -----
                    JSONObject userInfoJsonObject = JSON.parseObject(userInfo.toString());

                    // 构建新的请求头：将 userId 和 realName 添加到 header 中，供后续微服务使用
                    ServerHttpRequest.Builder builder = exchange.getRequest().mutate()
                            .headers(httpHeaders -> {
                                httpHeaders.set("userId", userInfoJsonObject.getString("id"));
                                // realName 进行 URL 编码，防止中文乱码
                                httpHeaders.set("realName", 
                                        URLEncoder.encode(userInfoJsonObject.getString("realName"), StandardCharsets.UTF_8));
                            });

                    // 使用修改后的请求继续执行过滤器链（放行）
                    return chain.filter(exchange.mutate().request(builder.build()).build());
                }

                // ----- Token 校验失败（缺失或无效），返回 401 未授权 -----
                ServerHttpResponse response = exchange.getResponse();
                response.setStatusCode(HttpStatus.UNAUTHORIZED);

                // 构建错误响应体（JSON 格式）
                return response.writeWith(Mono.fromSupplier(() -> {
                    DataBufferFactory bufferFactory = response.bufferFactory();
                    GatewayErrorResult resultMessage = GatewayErrorResult.builder()
                            .status(HttpStatus.UNAUTHORIZED.value())
                            .message("Token validation error")
                            .build();
                    // 将错误对象转为 JSON 字节并包装成 DataBuffer
                    return bufferFactory.wrap(JSON.toJSONString(resultMessage).getBytes());
                }));
            }

            // ----- 白名单请求，直接放行（不校验 Token） -----
            return chain.filter(exchange);
        };
    }

    /**
     * 判断请求是否在白名单中（跳过 Token 校验）
     * 
     * 白名单分为两类：
     * 1. 配置的白名单：通过 yaml 中 whitePathList 配置的路径前缀（如 /api/short-link/admin/v1/user/login）
     *    只要请求路径以列表中任一前缀开头，即视为白名单。
     * 2. 硬编码的特殊注册接口：POST /api/short-link/admin/v1/user （用户注册接口）也放行。
     * 
     * @param requestPath   请求路径
     * @param requestMethod 请求方法（GET/POST 等）
     * @param whitePathList 配置文件传入的白名单列表
     * @return true 表示在白名单中，无需校验 Token；false 表示需要校验
     */
    private boolean isPathInWhiteList(String requestPath, String requestMethod, List<String> whitePathList) {
        // 条件1：配置的白名单（前缀匹配）
        boolean inConfigWhitelist = !CollectionUtils.isEmpty(whitePathList) 
                && whitePathList.stream().anyMatch(requestPath::startsWith);

        // 条件2：硬编码的注册接口（路径完全匹配且方法为 POST）
        boolean isRegisterEndpoint = Objects.equals(requestPath, "/api/short-link/admin/v1/user") 
                && Objects.equals(requestMethod, "POST");

        return inConfigWhitelist || isRegisterEndpoint;
    }
}