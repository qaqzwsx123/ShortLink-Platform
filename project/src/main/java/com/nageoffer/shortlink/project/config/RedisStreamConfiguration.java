/*
 * 开源许可证声明（省略，已保留）
 */

package com.nageoffer.shortlink.project.config;

import com.nageoffer.shortlink.project.mq.consumer.ShortLinkStatsSaveConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_GROUP_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

/**
 * Redis Stream 消息队列配置
 * 
 * 本配置类用于搭建基于 Redis Stream 的异步统计消费基础设施。
 * 生产者将短链接访问事件写入 Stream，消费者异步拉取并写入 MySQL 多张统计表，
 * 实现跳转主链路与统计落库的解耦，提升系统吞吐量。
 * 
 */
@Configuration
@RequiredArgsConstructor
public class RedisStreamConfiguration {

    private final RedisConnectionFactory redisConnectionFactory;
    private final ShortLinkStatsSaveConsumer shortLinkStatsSaveConsumer;

    /**
     * 自定义消费线程池
     * 
     * 该线程池负责执行 Stream 消息拉取后的业务处理（即调用 Consumer 的 onMessage）。
     * 当前配置特点：
     * - 核心线程数和最大线程数均为 1（单线程消费），保证消息的顺序性，但吞吐量受限。
     * - 使用 SynchronousQueue（容量为 0 的阻塞队列），意味着如果没有空闲线程处理新任务，
     *   会立即触发拒绝策略。
     * - 拒绝策略采用 DiscardOldestPolicy：当任务被拒绝时，丢弃队列中最老的任务（如果有），
     *   然后尝试提交新任务。此策略在积压时会造成统计任务丢失，需谨慎评估。
     * - 线程设置为守护线程，应用退出时自动终止。
     * 
     * 生产环境建议：
     * - 根据业务 QPS 和统计表写入耗时，调整核心线程数（如 CPU 核心数 * 2），
     *   并使用有界队列（如 LinkedBlockingQueue）避免任务丢弃。
     * - 监控线程池活跃度和队列长度，及时告警。
     */
    @Bean
    public ExecutorService asyncStreamConsumer() {
        AtomicInteger index = new AtomicInteger();
        return new ThreadPoolExecutor(
                1,                               // corePoolSize
                1,                               // maximumPoolSize
                60,                              // keepAliveTime（空闲线程存活时间）
                TimeUnit.SECONDS,                // 时间单位
                new SynchronousQueue<>(),        // 直接传递队列，无缓冲
                runnable -> {                    // 线程工厂：自定义线程名
                    Thread thread = new Thread(runnable);
                    thread.setName("stream_consumer_short-link_stats_" + index.incrementAndGet());
                    thread.setDaemon(true);      // 守护线程，不影响 JVM 正常退出
                    return thread;
                },
                new ThreadPoolExecutor.DiscardOldestPolicy()  // 拒绝策略：丢弃最老的任务
        );
    }

    /**
     * 注册 Redis Stream 消费者订阅
     * 
     * 该方法创建并启动一个 StreamMessageListenerContainer，用于持续监听指定 Stream。
     * 容器内部会周期性调用 XREADGROUP 命令，拉取属于当前消费组且尚未确认的消息。
     * 
     * 关键配置解析：
     * 1. batchSize(10)：
     *    每次拉取最多 10 条消息。过小会增加网络往返，过大可能造成单次处理压力，
     *    需根据统计写入的 TPS 和单条处理耗时调整。
     * 2. pollTimeout(Duration.ofSeconds(3))：
     *    当 Stream 中无新消息时，XREADGROUP 阻塞等待的最大时间。避免空轮询浪费 CPU，
     *    同时防止超过 spring.redis.timeout 配置导致连接超时。
     * 3. executor(asyncStreamConsumer)：
     *    指定自定义线程池执行消息处理，而非使用容器默认的线程池。
     * 4. ReadOffset.lastConsumed()：
     *    从消费组上次已确认的偏移量之后继续读取，实现断点续传。
     *    重启后不会重复消费已确认的消息（前提是 autoAcknowledge 行为符合预期）。
     * 5. consumer(Consumer.from(GROUP, "stats-consumer"))：
     *    指定消费组名和消费者名称。同一消费组内多个消费者可实现负载均衡，
     *    当前只有单一消费者，故顺序消费。
     * 6. autoAcknowledge(true)：
     *    消息被拉取后立即自动 ACK（确认），即 Redis 将该消息从 Pending 列表中移除，
     *    并更新消费组偏移量。此操作发生在调用 onMessage 之前。
     * 
     * ⚠️ 风险提示：
     * - autoAcknowledge(true) 与「写 MySQL 成功」之间没有事务保证。
     *   如果消息拉取后、实际落库前发生异常或宕机，该消息将永久丢失（因为已被 ACK，
     *   不会重新投递）。生产环境中建议改为手动 ACK，在业务处理成功后再调用 acknowledge。
     * - 当前使用单线程 + 同步阻塞的统计写入，若某一批消息处理慢，会阻塞后续拉取，
     *   导致 Stream 积压。可考虑使用多消费者或异步写入提升并发。
     * - 缺少失败重试和死信机制，异常消息会直接抛出，需在外层捕获并记录，必要时
     *   转入人工处理或死信队列。
     * 
     * 改进方向：
     * - 将 autoAcknowledge 设为 false，在 actualSaveShortLinkStats 成功后手动 ACK。
     * - 实现 Pending 消息自动恢复（如定时扫描长时间未处理的消息并重新认领）。
     * - 增加监控：Stream 长度、Pending 数量、消费延迟等。
     */
    @Bean
    public Subscription shortLinkStatsSaveConsumerSubscription(ExecutorService asyncStreamConsumer) {
        // 1. 配置监听容器选项
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        .batchSize(10)                        // 每次拉取消息批量大小
                        .executor(asyncStreamConsumer)        // 执行业务逻辑的线程池
                        .pollTimeout(Duration.ofSeconds(3))   // 拉取超时时间（阻塞等待）
                        .build();

        // 2. 构建读取请求
        //    - StreamOffset.create(topic, ReadOffset.lastConsumed())：从消费组上次确认的位置继续消费
        //    - cancelOnError(false)：当发生错误时不取消订阅，继续尝试拉取
        //    - consumer：指定消费组和消费者名
        //    - autoAcknowledge(true)：自动确认（见上述风险说明）
        StreamMessageListenerContainer.StreamReadRequest<String> streamReadRequest =
                StreamMessageListenerContainer.StreamReadRequest
                        .builder(StreamOffset.create(SHORT_LINK_STATS_STREAM_TOPIC_KEY, ReadOffset.lastConsumed()))
                        .cancelOnError(throwable -> false)    // 出错不取消订阅
                        .consumer(Consumer.from(SHORT_LINK_STATS_STREAM_GROUP_KEY, "stats-consumer"))
                        .autoAcknowledge(true)               // 自动确认（生产环境建议改为 false）
                        .build();

        // 3. 创建并启动监听容器
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer =
                StreamMessageListenerContainer.create(redisConnectionFactory, options);

        // 注册消息监听器（即 ShortLinkStatsSaveConsumer 的 onMessage 方法）
        Subscription subscription = listenerContainer.register(streamReadRequest, shortLinkStatsSaveConsumer);
        
        // 容器启动后，会开始循环拉取消息并分发给监听器
        listenerContainer.start();

        // 返回 Subscription 对象，可用于后续取消订阅或状态查询
        return subscription;
    }
}