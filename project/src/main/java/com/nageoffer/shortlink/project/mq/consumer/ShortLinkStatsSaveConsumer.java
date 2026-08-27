package com.nageoffer.shortlink.project.mq.consumer;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessLogsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkBrowserStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkDeviceStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkLocaleStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkNetworkStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkOsStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsTodayDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkGotoDO;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessLogsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkBrowserStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkDeviceStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkLocaleStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkNetworkStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkOsStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkStatsTodayMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.mq.idempotent.MessageQueueIdempotentHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GID_UPDATE_KEY;
import static com.nageoffer.shortlink.project.common.constant.ShortLinkConstant.AMAP_REMOTE_URL;

/**
 * 短链接监控状态保存消息队列消费者
 * <p>
 * 失败消息隔离方案：重试计数 + 死信队列（DLQ）
 * 1. 每条消息最多重试 MAX_RETRY_COUNT 次（默认 3 次）
 * 2. 重试次数未达上限：清除幂等标记，抛异常让消息重新投递
 * 3. 重试次数达到上限：写入死信 Stream，ACK 原消息，避免无限重试和阻塞主队列
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShortLinkStatsSaveConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    // ==================== 失败消息隔离相关常量 ====================

    /**
     * 消息重试计数 Key 前缀
     * 完整 Key：retry_count:{messageId}
     */
    private static final String MESSAGE_RETRY_COUNT_KEY = "retry_count:%s";

    /**
     * 死信队列 Stream 名称
     */
    private static final String DEAD_LETTER_STREAM_KEY = "short_link_stats_dlq";

    /**
     * 最大重试次数
     */
    private static final int MAX_RETRY_COUNT = 3;

    /**
     * 重试计数过期时间（小时）
     */
    private static final long RETRY_COUNT_EXPIRE_HOURS = 24;

    // ==================== 依赖注入 ====================

    private final ShortLinkMapper shortLinkMapper;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final RedissonClient redissonClient;
    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final LinkStatsTodayMapper linkStatsTodayMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final MessageQueueIdempotentHandler messageQueueIdempotentHandler;

    @Value("${short-link.stats.locale.amap-key}")
    private String statsLocaleAmapKey;

    // ==================== 消息消费入口 ====================

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String stream = message.getStream();
        RecordId id = message.getId();
        String messageId = id.toString();

        // ============ 1. 幂等检查 ============
        // 使用 Redis 幂等标记，防止同一消息重复执行统计写入
        if (messageQueueIdempotentHandler.isMessageBeingConsumed(messageId)) {
            // 判断当前消息流程是否已执行完成
            if (messageQueueIdempotentHandler.isAccomplish(messageId)) {
                return;
            }
            // 正在被其他线程处理，本次不重复处理，触发重试
            throw new ServiceException("消息未完成流程，需要消息队列重试");
        }

        try {
            Map<String, String> producerMap = message.getValue();
            ShortLinkStatsRecordDTO statsRecord = JSON.parseObject(producerMap.get("statsRecord"), ShortLinkStatsRecordDTO.class);

            // 真正执行统计数据的聚合/落库写入
            actualSaveShortLinkStats(statsRecord);

            // 消费成功后，从 Redis Stream 中删除该条消息（Stream 为日志结构只进不出，
            // 必须手动 ACK 删除，否则消息会一直堆积）
            stringRedisTemplate.opsForStream().delete(Objects.requireNonNull(stream), id.getValue());

            // 清理重试计数（本次消费已成功，重试计数无用）
            clearRetryCount(messageId);

            // 标记完成：作为后续重复消息幂等判定的依据
            messageQueueIdempotentHandler.setAccomplish(messageId);

        } catch (Throwable ex) {
            // ============ 2. 消费失败：进入重试/死信流程 ============
            handleConsumeFailure(stream, message, id, messageId, ex);
        }
    }

    // ==================== 失败处理核心方法 ====================

    /**
     * 处理消费失败：判断重试次数，决定是继续重试还是进入死信队列
     *
     * @param stream    Stream 名称
     * @param message   原始消息
     * @param id        消息 ID
     * @param messageId 消息 ID 字符串
     * @param ex        异常
     */
    private void handleConsumeFailure(String stream, MapRecord<String, String, String> message,
                                      RecordId id, String messageId, Throwable ex) {
        // 获取当前重试次数
        int currentRetryCount = getRetryCount(messageId);

        if (currentRetryCount < MAX_RETRY_COUNT) {
            // ========== 情况一：未达到最大重试次数，继续重试 ==========
            int newRetryCount = currentRetryCount + 1;
            incrementRetryCount(messageId, newRetryCount);

            // 清除「处理中」标记，让消息可以被重新投递
            messageQueueIdempotentHandler.delMessageProcessed(messageId);

            log.error("消息消费失败，第 {}/{} 次重试，消息ID: {}，异常: {}",
                    newRetryCount, MAX_RETRY_COUNT, messageId, ex.getMessage(), ex);

            // 抛出异常，让 StreamMessageListenerContainer 感知消费失败
            // 消息会保留在 Pending 列表，等待下一次重新投递
            throw new ServiceException("消息消费失败，需要重试");

        } else {
            // ========== 情况二：达到最大重试次数，进入死信队列 ==========
            log.error("消息重试 {} 次后仍然失败，进入死信队列，消息ID: {}，原始消息: {}",
                    MAX_RETRY_COUNT, messageId, message.getValue(), ex);

            try {
                // 1. 构建死信消息，包含原始消息和错误信息，便于人工排查
                Map<String, String> dlqMessage = new HashMap<>();
                dlqMessage.put("original_message_id", messageId);
                dlqMessage.put("original_stream", stream);
                dlqMessage.put("original_value", JSON.toJSONString(message.getValue()));
                dlqMessage.put("error_type", ex.getClass().getName());
                dlqMessage.put("error_message", ex.getMessage());
                dlqMessage.put("failed_at", String.valueOf(System.currentTimeMillis()));
                dlqMessage.put("retry_count", String.valueOf(MAX_RETRY_COUNT));

                // 2. 写入死信队列
                stringRedisTemplate.opsForStream().add(DEAD_LETTER_STREAM_KEY, dlqMessage);

                // 3. 从原 Stream 中删除消息（ACK），防止毒消息阻塞主队列
                stringRedisTemplate.opsForStream().delete(Objects.requireNonNull(stream), id.getValue());

                // 4. 清除重试计数和幂等标记
                clearRetryCount(messageId);
                messageQueueIdempotentHandler.delMessageProcessed(messageId);

                // 5. 告警日志（可对接钉钉/企业微信/邮件等）
                log.error("【死信告警】短链接统计消息进入死信队列，需要人工介入！消息ID: {}", messageId);

            } catch (Exception dlqEx) {
                // 死信写入本身失败：仅记录日志，不再抛异常，避免死循环
                log.error("写入死信队列失败，消息ID: {}，原始异常: {}，死信写入异常: {}",
                        messageId, ex.getMessage(), dlqEx.getMessage(), dlqEx);
            }
        }
    }

    // ==================== 重试计数工具方法 ====================

    /**
     * 获取消息的重试次数
     *
     * @param messageId 消息 ID
     * @return 重试次数，不存在时返回 0
     */
    private int getRetryCount(String messageId) {
        String key = String.format(MESSAGE_RETRY_COUNT_KEY, messageId);
        String count = stringRedisTemplate.opsForValue().get(key);
        return count == null ? 0 : Integer.parseInt(count);
    }

    /**
     * 增加重试次数
     *
     * @param messageId 消息 ID
     * @param count     新的重试次数
     */
    private void incrementRetryCount(String messageId, int count) {
        String key = String.format(MESSAGE_RETRY_COUNT_KEY, messageId);
        stringRedisTemplate.opsForValue().set(key, String.valueOf(count),
                RETRY_COUNT_EXPIRE_HOURS, TimeUnit.HOURS);
    }

    /**
     * 清除重试计数（消费成功或进入死信后调用）
     *
     * @param messageId 消息 ID
     */
    private void clearRetryCount(String messageId) {
        String key = String.format(MESSAGE_RETRY_COUNT_KEY, messageId);
        stringRedisTemplate.delete(key);
    }

    // ==================== 实际统计写入（保持不变） ====================

    /**
     * 实际执行短链接统计数据保存的核心方法
     * 包括：基础访问统计、地域统计、操作系统、浏览器、设备、网络、访问日志、今日统计等
     *
     * @param statsRecord 统计数据传输对象，包含 PV/UV/UIP 标志、用户代理信息、IP 等
     */
    public void actualSaveShortLinkStats(ShortLinkStatsRecordDTO statsRecord) {
        // 1. 获取当前统计的完整短链接
        String fullShortUrl = statsRecord.getFullShortUrl();

        // 2. 获取读锁（与更新短链接时的写锁互斥，保证数据一致性）
        //    读锁允许并发读取，但会阻塞写锁，防止在统计过程中短链接的 Gid 被迁移
        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(
                String.format(LOCK_GID_UPDATE_KEY, fullShortUrl)
        );
        RLock rLock = readWriteLock.readLock();
        rLock.lock();  // 加锁，保证后续查询的 Gid 是准确且一致的

        try {
            // 3. 根据完整短链接查询对应的分组 Gid
            //    短链接可能会变更分组，因此需要从映射表中获取当前有效的 Gid
            LambdaQueryWrapper<ShortLinkGotoDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                    .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
            ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(queryWrapper);
            String gid = shortLinkGotoDO.getGid();

            // 4. 提取统计时间（精确到日）以及小时、星期等信息
            Date currentDate = statsRecord.getCurrentDate();
            int hour = DateUtil.hour(currentDate, true);          // 24小时制的小时数
            Week week = DateUtil.dayOfWeekEnum(currentDate);
            int weekValue = week.getIso8601Value();               // ISO标准星期几（周一=1）

            // 5. 保存基础访问统计（PV/UV/UIP）
            //    PV 每次 +1，UV 和 UIP 仅首次访问时 +1
            LinkAccessStatsDO linkAccessStatsDO = LinkAccessStatsDO.builder()
                    .pv(1)
                    .uv(statsRecord.getUvFirstFlag() ? 1 : 0)
                    .uip(statsRecord.getUipFirstFlag() ? 1 : 0)
                    .hour(hour)
                    .weekday(weekValue)
                    .fullShortUrl(fullShortUrl)
                    .date(currentDate)
                    .build();
            linkAccessStatsMapper.shortLinkStats(linkAccessStatsDO);

            // 6. 调用高德地图 API 根据 IP 获取地域信息（省份、城市）
            Map<String, Object> localeParamMap = new HashMap<>();
            localeParamMap.put("key", statsLocaleAmapKey);
            localeParamMap.put("ip", statsRecord.getRemoteAddr());
            String localeResultStr = HttpUtil.get(AMAP_REMOTE_URL, localeParamMap);
            JSONObject localeResultObj = JSON.parseObject(localeResultStr);
            String infoCode = localeResultObj.getString("infocode");

            // 默认未知地域
            String actualProvince = "未知";
            String actualCity = "未知";

            // 只有当 API 返回成功（infocode=10000）且省份不为空数组时，才更新地域信息
            if (StrUtil.isNotBlank(infoCode) && StrUtil.equals(infoCode, "10000")) {
                String province = localeResultObj.getString("province");
                boolean unknownFlag = StrUtil.equals(province, "[]");
                // 如果省份为空数组，则保持“未知”，否则取实际值
                LinkLocaleStatsDO linkLocaleStatsDO = LinkLocaleStatsDO.builder()
                        .province(actualProvince = unknownFlag ? actualProvince : province)
                        .city(actualCity = unknownFlag ? actualCity : localeResultObj.getString("city"))
                        .adcode(unknownFlag ? "未知" : localeResultObj.getString("adcode"))
                        .cnt(1)
                        .fullShortUrl(fullShortUrl)
                        .country("中国")
                        .date(currentDate)
                        .build();
                linkLocaleStatsMapper.shortLinkLocaleState(linkLocaleStatsDO);
            }

            // 7. 保存操作系统统计
            LinkOsStatsDO linkOsStatsDO = LinkOsStatsDO.builder()
                    .os(statsRecord.getOs())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(currentDate)
                    .build();
            linkOsStatsMapper.shortLinkOsState(linkOsStatsDO);

            // 8. 保存浏览器统计
            LinkBrowserStatsDO linkBrowserStatsDO = LinkBrowserStatsDO.builder()
                    .browser(statsRecord.getBrowser())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(currentDate)
                    .build();
            linkBrowserStatsMapper.shortLinkBrowserState(linkBrowserStatsDO);

            // 9. 保存设备类型统计（如移动/PC/平板）
            LinkDeviceStatsDO linkDeviceStatsDO = LinkDeviceStatsDO.builder()
                    .device(statsRecord.getDevice())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(currentDate)
                    .build();
            linkDeviceStatsMapper.shortLinkDeviceState(linkDeviceStatsDO);

            // 10. 保存网络类型统计（如WiFi/4G/5G）
            LinkNetworkStatsDO linkNetworkStatsDO = LinkNetworkStatsDO.builder()
                    .network(statsRecord.getNetwork())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(currentDate)
                    .build();
            linkNetworkStatsMapper.shortLinkNetworkState(linkNetworkStatsDO);

            // 11. 保存访问日志明细（包含用户标识、IP、UA等信息，用于后续详细分析）
            LinkAccessLogsDO linkAccessLogsDO = LinkAccessLogsDO.builder()
                    .user(statsRecord.getUv())
                    .ip(statsRecord.getRemoteAddr())
                    .browser(statsRecord.getBrowser())
                    .os(statsRecord.getOs())
                    .network(statsRecord.getNetwork())
                    .device(statsRecord.getDevice())
                    .locale(StrUtil.join("-", "中国", actualProvince, actualCity))
                    .fullShortUrl(fullShortUrl)
                    .build();
            linkAccessLogsMapper.insert(linkAccessLogsDO);

            // 12. 更新短链接主表中的总访问量、总UV、总UIP（用于短链接详情页展示）
            shortLinkMapper.incrementStats(
                    gid,
                    fullShortUrl,
                    1,                                    // PV +1
                    statsRecord.getUvFirstFlag() ? 1 : 0, // UV +1（首次）
                    statsRecord.getUipFirstFlag() ? 1 : 0 // UIP +1（首次）
            );

            // 13. 保存今日统计（用于今日实时看板）
            LinkStatsTodayDO linkStatsTodayDO = LinkStatsTodayDO.builder()
                    .todayPv(1)
                    .todayUv(statsRecord.getUvFirstFlag() ? 1 : 0)
                    .todayUip(statsRecord.getUipFirstFlag() ? 1 : 0)
                    .fullShortUrl(fullShortUrl)
                    .date(currentDate)
                    .build();
            linkStatsTodayMapper.shortLinkTodayState(linkStatsTodayDO);

        } finally {
            // 14. 释放读锁，确保其他线程可以继续操作
            rLock.unlock();
        }
    }
}