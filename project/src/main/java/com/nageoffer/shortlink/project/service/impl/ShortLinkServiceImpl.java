/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.shortlink.project.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.text.StrBuilder;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.shortlink.project.common.convention.exception.ClientException;
import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.common.enums.VailDateTypeEnum;
import com.nageoffer.shortlink.project.config.GotoDomainWhiteListConfiguration;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkGotoDO;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkBatchCreateReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkCreateReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkPageReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkUpdateReqDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkBaseInfoRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkBatchCreateRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkCreateRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkGroupCountQueryRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkPageRespDTO;
import com.nageoffer.shortlink.project.mq.producer.ShortLinkStatsSaveProducer;
import com.nageoffer.shortlink.project.service.ShortLinkService;
import com.nageoffer.shortlink.project.toolkit.HashUtil;
import com.nageoffer.shortlink.project.toolkit.LinkUtil;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_IS_NULL_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GID_UPDATE_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GOTO_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_CREATE_LOCK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_UIP_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_UV_KEY;

/**
 * 短链接接口实现层
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShortLinkServiceImpl extends ServiceImpl<ShortLinkMapper, ShortLinkDO> implements ShortLinkService {

    private final RBloomFilter<String> shortUriCreateCachePenetrationBloomFilter;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final ShortLinkStatsSaveProducer shortLinkStatsSaveProducer;
    private final GotoDomainWhiteListConfiguration gotoDomainWhiteListConfiguration;

    /** Redis 故障时限制数据库兜底并发，避免缓存雪崩拖垮数据库。 */
    private final Semaphore redisFallbackSemaphore = new Semaphore(100);

    @Value("${short-link.domain.default}")
    private String createShortLinkDefaultDomain;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public ShortLinkCreateRespDTO createShortLink(ShortLinkCreateReqDTO requestParam) {
        // 短链接接口的并发量有多少？如何测试？详情查看：https://nageoffer.com/shortlink/question
        // 主短链接表和跳转映射表在同一事务中写入，保证两者数据一致。
        verificationWhitelist(requestParam.getOriginUrl());
        String shortLinkSuffix = generateSuffix(requestParam);
        String fullShortUrl = StrBuilder.create(createShortLinkDefaultDomain)
                .append("/")
                .append(shortLinkSuffix)
                .toString();
        ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                .domain(createShortLinkDefaultDomain)
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .createdType(requestParam.getCreatedType())
                .validDateType(requestParam.getValidDateType())
                .validDate(requestParam.getValidDate())
                .describe(requestParam.getDescribe())
                .shortUri(shortLinkSuffix)
                .enableStatus(0)
                .totalPv(0)
                .totalUv(0)
                .totalUip(0)
                .delTime(0L)
                .fullShortUrl(fullShortUrl)
                .favicon(getFavicon(requestParam.getOriginUrl()))
                .build();
        ShortLinkGotoDO linkGotoDO = ShortLinkGotoDO.builder()
                .fullShortUrl(fullShortUrl)
                .gid(requestParam.getGid())
                .build();
        try {
            // 短链接项目有多少数据？如何解决海量数据存储？详情查看：https://nageoffer.com/shortlink/question
            baseMapper.insert(shortLinkDO);
            // 短链接数据库分片键是如何考虑的？详情查看：https://nageoffer.com/shortlink/question
            shortLinkGotoMapper.insert(linkGotoDO);
        } catch (DuplicateKeyException ex) {
            // 首先判断是否存在布隆过滤器，如果不存在直接新增
            if (!shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl)) {
                shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
            }
            throw new ServiceException(String.format("短链接：%s 生成重复", fullShortUrl));
        }
        // 项目中短链接缓存预热是怎么做的？详情查看：https://nageoffer.com/shortlink/question
        stringRedisTemplate.opsForValue().set(
                String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                requestParam.getOriginUrl(),
                LinkUtil.getLinkCacheValidTime(requestParam.getValidDate()), TimeUnit.MILLISECONDS);
        // 布隆过滤器不支持安全删除，短链接删除后仍保留标识；后续请求继续走数据库状态校验。
        shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
        return ShortLinkCreateRespDTO.builder()
                .fullShortUrl("http://" + shortLinkDO.getFullShortUrl())
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .build();
    }

    @Override
    public ShortLinkCreateRespDTO createShortLinkByLock(ShortLinkCreateReqDTO requestParam) {
        verificationWhitelist(requestParam.getOriginUrl());
        String fullShortUrl;
        // 为什么说布隆过滤器性能远胜于分布式锁？详情查看：https://nageoffer.com/shortlink/question
        RLock lock = redissonClient.getLock(SHORT_LINK_CREATE_LOCK_KEY);
        lock.lock();
        try {
            String shortLinkSuffix = generateSuffixByLock(requestParam);
            fullShortUrl = StrBuilder.create(createShortLinkDefaultDomain)
                    .append("/")
                    .append(shortLinkSuffix)
                    .toString();
            ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                    .domain(createShortLinkDefaultDomain)
                    .originUrl(requestParam.getOriginUrl())
                    .gid(requestParam.getGid())
                    .createdType(requestParam.getCreatedType())
                    .validDateType(requestParam.getValidDateType())
                    .validDate(requestParam.getValidDate())
                    .describe(requestParam.getDescribe())
                    .shortUri(shortLinkSuffix)
                    .enableStatus(0)
                    .totalPv(0)
                    .totalUv(0)
                    .totalUip(0)
                    .delTime(0L)
                    .fullShortUrl(fullShortUrl)
                    .favicon(getFavicon(requestParam.getOriginUrl()))
                    .build();
            ShortLinkGotoDO linkGotoDO = ShortLinkGotoDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .gid(requestParam.getGid())
                    .build();
            try {
                baseMapper.insert(shortLinkDO);
                shortLinkGotoMapper.insert(linkGotoDO);
            } catch (DuplicateKeyException ex) {
                throw new ServiceException(String.format("短链接：%s 生成重复", fullShortUrl));
            }
            stringRedisTemplate.opsForValue().set(
                    String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                    requestParam.getOriginUrl(),
                    LinkUtil.getLinkCacheValidTime(requestParam.getValidDate()), TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
        return ShortLinkCreateRespDTO.builder()
                .fullShortUrl("http://" + fullShortUrl)
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .build();
    }

    @Override
    public ShortLinkBatchCreateRespDTO batchCreateShortLink(ShortLinkBatchCreateReqDTO requestParam) {
        List<String> originUrls = requestParam.getOriginUrls();
        List<String> describes = requestParam.getDescribes();
        List<ShortLinkBaseInfoRespDTO> result = new ArrayList<>();
        for (int i = 0; i < originUrls.size(); i++) {
            ShortLinkCreateReqDTO shortLinkCreateReqDTO = BeanUtil.toBean(requestParam, ShortLinkCreateReqDTO.class);
            shortLinkCreateReqDTO.setOriginUrl(originUrls.get(i));
            shortLinkCreateReqDTO.setDescribe(describes.get(i));
            try {
                ShortLinkCreateRespDTO shortLink = createShortLink(shortLinkCreateReqDTO);
                ShortLinkBaseInfoRespDTO linkBaseInfoRespDTO = ShortLinkBaseInfoRespDTO.builder()
                        .fullShortUrl(shortLink.getFullShortUrl())
                        .originUrl(shortLink.getOriginUrl())
                        .describe(describes.get(i))
                        .build();
                result.add(linkBaseInfoRespDTO);
            } catch (Throwable ex) {
                log.error("批量创建短链接失败，原始参数：{}", originUrls.get(i));
            }
        }
        return ShortLinkBatchCreateRespDTO.builder()
                .total(result.size())
                .baseLinkInfos(result)
                .build();
    }

    /**
     * 更新短链接信息（支持修改目标URL、有效期、描述，以及分组迁移）
     * 
     * 整个方法使用 @Transactional 保证数据库操作的一致性，
     * rollbackFor = Exception.class 表示任何异常都回滚事务（包括受检异常）。
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void updateShortLink(ShortLinkUpdateReqDTO requestParam) {
        // 1. 安全校验：验证原始长链接是否在白名单内（防止外链劫持或违规域名）
        verificationWhitelist(requestParam.getOriginUrl());

        // 2. 查询当前短链接记录（根据原分组 gid 和完整短链接）
        LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                .eq(ShortLinkDO::getGid, requestParam.getOriginGid())
                .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                .eq(ShortLinkDO::getDelFlag, 0) // 未删除
                .eq(ShortLinkDO::getEnableStatus, 0); // 启用状态
        ShortLinkDO hasShortLinkDO = baseMapper.selectOne(queryWrapper);

        // 若记录不存在，直接抛出业务异常（事务回滚）
        if (hasShortLinkDO == null) {
            throw new ClientException("短链接记录不存在");
        }

        // 3. 判断是否发生分组迁移（gid 是否变化）
        if (Objects.equals(hasShortLinkDO.getGid(), requestParam.getGid())) {
            // ---------- 情况一：分组未变更，直接在原记录上更新 ----------
            // 构造更新条件（根据 gid 和 fullShortUrl 定位唯一记录）
            LambdaUpdateWrapper<ShortLinkDO> updateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                    .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                    .eq(ShortLinkDO::getGid, requestParam.getGid())
                    .eq(ShortLinkDO::getDelFlag, 0)
                    .eq(ShortLinkDO::getEnableStatus, 0)
                    // 若有效期类型为永久，则将 validDate 设为 null（数据库字段可为空）
                    .set(Objects.equals(requestParam.getValidDateType(), VailDateTypeEnum.PERMANENT.getType()),
                            ShortLinkDO::getValidDate, null);

            // 构造要更新的实体对象（只包含需要修改的字段）
            ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                    .domain(hasShortLinkDO.getDomain()) // 域名不变
                    .shortUri(hasShortLinkDO.getShortUri()) // 短码不变
                    // 如果原URL未变，则沿用旧的 favicon，否则重新获取
                    .favicon(Objects.equals(requestParam.getOriginUrl(), hasShortLinkDO.getOriginUrl())
                            ? hasShortLinkDO.getFavicon()
                            : getFavicon(requestParam.getOriginUrl()))
                    .createdType(hasShortLinkDO.getCreatedType()) // 创建方式不变
                    .gid(requestParam.getGid()) // gid 未变（等价于原值）
                    .originUrl(requestParam.getOriginUrl()) // 可能更新
                    .describe(requestParam.getDescribe()) // 可能更新描述
                    .validDateType(requestParam.getValidDateType()) // 可能更新有效期类型
                    .validDate(requestParam.getValidDate()) // 可能更新有效期具体时间
                    .build();
            // 执行更新（根据 updateWrapper 中的条件匹配记录）
            baseMapper.update(shortLinkDO, updateWrapper);
        } else {
            // ---------- 情况二：分组发生变更（迁移到新分组） ----------
            // 为什么监控表要加上 Gid？不加的话是否就不存在读写锁？
            // 详情参考：https://nageoffer.com/shortlink/question
            // 简要说明：统计消费端（读）会按 gid 分组查询访问数据，
            // 若更新时直接移动记录而不加锁，会导致统计读数与当前数据不一致。
            // 因此使用读写锁协调并发：更新操作获取写锁，统计查询获取读锁。

            // 获取针对该短链接的读写锁（Redisson 分布式可重入读写锁）
            RReadWriteLock readWriteLock = redissonClient
                    .getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, requestParam.getFullShortUrl()));
            RLock rLock = readWriteLock.writeLock(); // 获取写锁
            rLock.lock();
            try {
                // 3.1 软删除原分组下的旧记录（逻辑删除，标记 del_flag=1）
                LambdaUpdateWrapper<ShortLinkDO> linkUpdateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                        .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                        .eq(ShortLinkDO::getGid, hasShortLinkDO.getGid()) // 原 gid
                        .eq(ShortLinkDO::getDelFlag, 0)
                        .eq(ShortLinkDO::getDelTime, 0L) // 确保尚未被删除
                        .eq(ShortLinkDO::getEnableStatus, 0);
                ShortLinkDO delShortLinkDO = ShortLinkDO.builder()
                        .delTime(System.currentTimeMillis()) // 记录删除时间
                        .build();
                delShortLinkDO.setDelFlag(1); // 标记为已删除
                baseMapper.update(delShortLinkDO, linkUpdateWrapper);

                // 3.2 在新分组下插入一条新记录（除 gid 外，其他业务字段与旧记录保持一致，但更新了 originUrl 等）
                ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                        .domain(createShortLinkDefaultDomain) // 固定域名（可配置）
                        .originUrl(requestParam.getOriginUrl())
                        .gid(requestParam.getGid()) // 新 gid
                        .createdType(hasShortLinkDO.getCreatedType())
                        .validDateType(requestParam.getValidDateType())
                        .validDate(requestParam.getValidDate())
                        .describe(requestParam.getDescribe())
                        .shortUri(hasShortLinkDO.getShortUri()) // 短码保持不变
                        .enableStatus(hasShortLinkDO.getEnableStatus())
                        .totalPv(hasShortLinkDO.getTotalPv()) // 保留原统计数据
                        .totalUv(hasShortLinkDO.getTotalUv())
                        .totalUip(hasShortLinkDO.getTotalUip())
                        .fullShortUrl(hasShortLinkDO.getFullShortUrl()) // 完整短链接不变
                        .favicon(Objects.equals(requestParam.getOriginUrl(), hasShortLinkDO.getOriginUrl())
                                ? hasShortLinkDO.getFavicon()
                                : getFavicon(requestParam.getOriginUrl()))
                        .delTime(0L) // 新记录未删除
                        .build();
                baseMapper.insert(shortLinkDO);

                // 3.3 更新路由表 t_link_goto：删除旧 gid 的记录，插入新 gid 的记录
                LambdaQueryWrapper<ShortLinkGotoDO> linkGotoQueryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                        .eq(ShortLinkGotoDO::getFullShortUrl, requestParam.getFullShortUrl())
                        .eq(ShortLinkGotoDO::getGid, hasShortLinkDO.getGid()); // 原 gid
                ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(linkGotoQueryWrapper);
                // 删除旧路由记录
                shortLinkGotoMapper.delete(linkGotoQueryWrapper);
                // 更新 gid 后重新插入
                shortLinkGotoDO.setGid(requestParam.getGid());
                shortLinkGotoMapper.insert(shortLinkGotoDO);
            } finally {
                rLock.unlock(); // 释放写锁，确保死锁不发生
            }
        }

        // ============ 4. 缓存清理（保证缓存与数据库最终一致性）============
        // 只有在有效期或原始 URL 发生变化时才需要删除缓存，
        // 如果仅修改描述等非关键字段，则无需清缓存（不影响跳转结果）。
        if (!Objects.equals(hasShortLinkDO.getValidDateType(), requestParam.getValidDateType())
                || !Objects.equals(hasShortLinkDO.getValidDate(), requestParam.getValidDate())
                || !Objects.equals(hasShortLinkDO.getOriginUrl(), requestParam.getOriginUrl())) {
            // 4.1 删除正向缓存（短链接 -> 长链接）
            stringRedisTemplate.delete(String.format(GOTO_SHORT_LINK_KEY, requestParam.getFullShortUrl()));

            // 4.2 如果旧链接已过期，而新设置变为永久或未来有效期，则需要删除可能存在的空值缓存
            // 因为之前过期时可能缓存了空值标记（-），现在链接复活了，必须删掉空值缓存。
            Date currentDate = new Date();
            if (hasShortLinkDO.getValidDate() != null && hasShortLinkDO.getValidDate().before(currentDate)) {
                if (Objects.equals(requestParam.getValidDateType(), VailDateTypeEnum.PERMANENT.getType())
                        || requestParam.getValidDate().after(currentDate)) {
                    // 空值缓存 key 与正向缓存 key 不同，前缀为 GOTO_IS_NULL_SHORT_LINK_KEY
                    stringRedisTemplate
                            .delete(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, requestParam.getFullShortUrl()));
                }
            }
        }
        // 注意：此处没有手动更新缓存（SET），而是采用 Cache Aside 的“只删缓存”策略，
        // 等待下次用户访问时通过 restoreUrl 方法触发缓存回填，从而保证最终一致性。
    }

    @Override
    public IPage<ShortLinkPageRespDTO> pageShortLink(ShortLinkPageReqDTO requestParam) {
        IPage<ShortLinkDO> resultPage = baseMapper.pageLink(requestParam);
        return resultPage.convert(each -> {
            ShortLinkPageRespDTO result = BeanUtil.toBean(each, ShortLinkPageRespDTO.class);
            result.setDomain("http://" + result.getDomain());
            return result;
        });
    }

    @Override
    public List<ShortLinkGroupCountQueryRespDTO> listGroupShortLinkCount(List<String> requestParam) {
        QueryWrapper<ShortLinkDO> queryWrapper = Wrappers.query(new ShortLinkDO())
                .select("gid as gid, count(*) as shortLinkCount")
                .in("gid", requestParam)
                .eq("enable_status", 0)
                .eq("del_flag", 0)
                .eq("del_time", 0L)
                .groupBy("gid");
        List<Map<String, Object>> shortLinkDOList = baseMapper.selectMaps(queryWrapper);
        return BeanUtil.copyToList(shortLinkDOList, ShortLinkGroupCountQueryRespDTO.class);
    }

    @SneakyThrows
    @Override
    public void restoreUrl(String shortUri, ServletRequest request, ServletResponse response) {
        // 1. 构建完整短链接（包含域名和端口），用于后续缓存和数据库查询的 Key
        String serverName = request.getServerName();
        String serverPort = Optional.of(request.getServerPort())
                .filter(each -> !Objects.equals(each, 80)) // 默认 80 端口不显示
                .map(String::valueOf)
                .map(each -> ":" + each)
                .orElse("");
        String fullShortUrl = serverName + serverPort + "/" + shortUri;

        // ============ 第一步：Cache Aside 模式 – 先查 Redis 映射缓存 ============
        // 目的：绝大多数合法短链接的跳转请求都应直接命中缓存，避免数据库压力。
        String originalLink;
        try {
            originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
        } catch (Exception ex) {
            log.warn("Redis 缓存读取失败，降级查询数据库，fullShortUrl={}", fullShortUrl, ex);
            restoreUrlByDatabaseFallback(fullShortUrl, request, response);
            return;
        }
        if (StrUtil.isNotBlank(originalLink)) {
            // 命中：记录统计事件（异步），并发送 302 重定向
            safeShortLinkStats(fullShortUrl, request, response);
            ((HttpServletResponse) response).sendRedirect(originalLink);
            return;
        }

        // ============ 第二步：布隆过滤器快速判断是否存在 ============
        // 目的：缓存未命中时，通过布隆过滤器判断该短链接是否可能存在于数据库。
        // 若过滤器明确表示“不存在”，则直接返回 404，避免对数据库的无效查询（防穿透）。
        // 注：布隆过滤器存在假阳性（可能误判存在），但绝不会漏判（不存在则一定不存在）。
        boolean contains;
        try {
            contains = shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl);
        } catch (Exception ex) {
            log.warn("Redis BloomFilter 不可用，降级查询数据库，fullShortUrl={}", fullShortUrl, ex);
            restoreUrlByDatabaseFallback(fullShortUrl, request, response);
            return;
        }
        if (!contains) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }

        // ============ 第三步：检查“空值缓存” ============
        // 目的：如果之前查库发现该链接不存在（可能因布隆过滤器假阳性导致），
        // 我们会缓存一个空值标记（-），有效期内直接返回 404，避免反复查库。
        String gotoIsNullShortLink;
        try {
            gotoIsNullShortLink = stringRedisTemplate.opsForValue()
                    .get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
        } catch (Exception ex) {
            log.warn("Redis 空值缓存读取失败，降级查询数据库，fullShortUrl={}", fullShortUrl, ex);
            restoreUrlByDatabaseFallback(fullShortUrl, request, response);
            return;
        }
        if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }

        // ============ 第四步：热点 Key 回源 – 分布式锁 ============
        // 目的：缓存失效且确认存在时，多个并发请求可能同时查询数据库，造成“缓存击穿”。
        // 此处按 fullShortUrl 粒度加 Redisson 分布式锁，保证只有一个线程回源查库，
        // 其他线程等待锁释放后再次检查缓存（双重检查）。
        RLock lock = redissonClient.getLock(String.format(LOCK_GOTO_SHORT_LINK_KEY, fullShortUrl));
        boolean locked;
        try {
            // 只设置等待时间，不传 leaseTime，继续使用 Redisson 看门狗自动续期。
            locked = lock.tryLock(100, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            log.warn("Redis 分布式锁不可用，降级查询数据库，fullShortUrl={}", fullShortUrl, ex);
            restoreUrlByDatabaseFallback(fullShortUrl, request, response);
            return;
        }
        if (!locked) {
            log.warn("获取短链接回源锁超时，降级查询数据库，fullShortUrl={}", fullShortUrl);
            restoreUrlByDatabaseFallback(fullShortUrl, request, response);
            return;
        }
        try {
            // ---------- 双重检查：再次查询缓存（可能已被其他线程填充） ----------
            try {
                originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
                gotoIsNullShortLink = stringRedisTemplate.opsForValue()
                        .get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
            } catch (Exception ex) {
                log.warn("回源过程 Redis 二次检查失败，降级查询数据库，fullShortUrl={}", fullShortUrl, ex);
                restoreUrlByDatabaseFallback(fullShortUrl, request, response);
                return;
            }
            if (StrUtil.isNotBlank(originalLink)) {
                // 其他线程已回源成功，直接使用缓存结果
                safeShortLinkStats(fullShortUrl, request, response);
                ((HttpServletResponse) response).sendRedirect(originalLink);
                return;
            }
            // 再次检查空值缓存（可能其他线程已缓存“不存在”）
            if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }

            // ---------- 查询数据库：先查 `t_link_goto` 表获取分组 ID（gid） ----------
            // 设计上使用 `t_link_goto` 作为路由表，通过 fullShortUrl 快速定位 gid，
            // 再结合 gid 和 fullShortUrl 查询主表 `t_link`，支持分库分表场景。
            LambdaQueryWrapper<ShortLinkGotoDO> linkGotoQueryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                    .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
            ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(linkGotoQueryWrapper);

            // 路由表无记录 → 该短链接确实不存在（布隆过滤器假阳性触发）
            if (shortLinkGotoDO == null) {
                // 缓存空值 30 分钟，防止短时间内大量相同请求穿透到数据库
                cacheNullValue(fullShortUrl);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }

            // ---------- 查询主表 `t_link` ----------
            LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                    .eq(ShortLinkDO::getGid, shortLinkGotoDO.getGid())
                    .eq(ShortLinkDO::getFullShortUrl, fullShortUrl)
                    .eq(ShortLinkDO::getDelFlag, 0) // 未删除
                    .eq(ShortLinkDO::getEnableStatus, 0); // 启用状态
            ShortLinkDO shortLinkDO = baseMapper.selectOne(queryWrapper);

            // 主表无记录，或链接已过期
            if (shortLinkDO == null
                    || (shortLinkDO.getValidDate() != null && shortLinkDO.getValidDate().before(new Date()))) {
                // 同样缓存空值标记，避免过期链接被频繁访问
                cacheNullValue(fullShortUrl);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }

            // ---------- 回填缓存 ----------
            // 将原始长链接存入 Redis，并设置恰当的过期时间（根据链接有效期动态计算）
            try {
                stringRedisTemplate.opsForValue().set(
                        String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                        shortLinkDO.getOriginUrl(),
                        LinkUtil.getLinkCacheValidTime(shortLinkDO.getValidDate()),
                        TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                log.warn("Redis 缓存回填失败，继续返回数据库结果，fullShortUrl={}", fullShortUrl, ex);
            }

            // 记录统计事件（异步写入 Redis Stream，最终落库）
            safeShortLinkStats(fullShortUrl, request, response);

            // 执行 302 重定向到原始长链接
            ((HttpServletResponse) response).sendRedirect(shortLinkDO.getOriginUrl());

        } finally {
            // 无论成功或异常，确保释放分布式锁，避免死锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void cacheNullValue(String fullShortUrl) {
        try {
            stringRedisTemplate.opsForValue().set(
                    String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl),
                    "-",
                    30,
                    TimeUnit.MINUTES);
        } catch (Exception ex) {
            log.warn("Redis 空值缓存写入失败，fullShortUrl={}", fullShortUrl, ex);
        }
    }

    /** Redis 故障期间的受控数据库兜底，不回填 Redis，避免继续放大故障。 */
    @SneakyThrows
    private void restoreUrlByDatabaseFallback(String fullShortUrl, ServletRequest request, ServletResponse response) {
        if (!redisFallbackSemaphore.tryAcquire(100, TimeUnit.MILLISECONDS)) {
            ((HttpServletResponse) response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return;
        }
        try {
            ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(
                    Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                            .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl));
            if (shortLinkGotoDO == null) {
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            ShortLinkDO shortLinkDO = baseMapper.selectOne(
                    Wrappers.lambdaQuery(ShortLinkDO.class)
                            .eq(ShortLinkDO::getGid, shortLinkGotoDO.getGid())
                            .eq(ShortLinkDO::getFullShortUrl, fullShortUrl)
                            .eq(ShortLinkDO::getDelFlag, 0)
                            .eq(ShortLinkDO::getEnableStatus, 0));
            if (shortLinkDO == null
                    || (shortLinkDO.getValidDate() != null && shortLinkDO.getValidDate().before(new Date()))) {
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            safeShortLinkStats(fullShortUrl, request, response);
            ((HttpServletResponse) response).sendRedirect(shortLinkDO.getOriginUrl());
        } finally {
            redisFallbackSemaphore.release();
        }
    }

    /** 统计属于非核心链路，Redis/Stream 异常不能阻断短链接跳转。 */
    private void safeShortLinkStats(String fullShortUrl, ServletRequest request, ServletResponse response) {
        try {
            shortLinkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
        } catch (Exception ex) {
            log.warn("短链接统计降级丢弃，fullShortUrl={}", fullShortUrl, ex);
        }
    }

    /**
     * 构建短链接统计记录，并设置用户标识（UV）
     * 该方法完成以下工作：
     * 1. 尝试从 Cookie 中获取 UV 标识，若无则生成新 UUID 并写入 Cookie
     * 2. 使用 Redis Set 记录 UV，并判断是否首次出现（uvFirstFlag）
     * 3. 获取客户端 IP、操作系统、浏览器、设备、网络类型等信息
     * 4. 使用 Redis Set 记录 UIP（独立IP），并判断是否首次出现（uipFirstFlag）
     * 5. 组装并返回 ShortLinkStatsRecordDTO 对象，供异步统计消费
     *
     * @param fullShortUrl 完整短链接（如：nageoffer.com/abc123）
     * @param request      ServletRequest 对象，用于获取客户端信息
     * @param response     ServletResponse 对象，用于写入 Cookie
     * @return 统计记录 DTO
     */
    private ShortLinkStatsRecordDTO buildLinkStatsRecordAndSetUser(String fullShortUrl, ServletRequest request,
            ServletResponse response) {
        // 用于标记 UV 是否首次出现（今日或本次统计周期内）
        AtomicBoolean uvFirstFlag = new AtomicBoolean();
        // 获取请求中的所有 Cookie
        Cookie[] cookies = ((HttpServletRequest) request).getCookies();
        // 用于存放 UV 值（用户唯一标识）
        AtomicReference<String> uv = new AtomicReference<>();

        // 定义一个任务：生成新的 UV（UUID），写入 Cookie，并标记为首次出现，同时将 UV 存入 Redis Set
        Runnable addResponseCookieTask = () -> {
            // 生成随机 UUID 作为用户唯一标识
            uv.set(UUID.fastUUID().toString());
            // 创建名为 "uv" 的 Cookie，有效期为 30 天
            Cookie uvCookie = new Cookie("uv", uv.get());
            uvCookie.setMaxAge(60 * 60 * 24 * 30); // 30天
            // 设置 Cookie 路径为短链接的路径部分（如 /abc123），确保只对该短链接生效
            uvCookie.setPath(StrUtil.sub(fullShortUrl, fullShortUrl.indexOf("/"), fullShortUrl.length()));
            ((HttpServletResponse) response).addCookie(uvCookie);
            // 新生成的 UV 视为首次访问
            uvFirstFlag.set(Boolean.TRUE);
            // 将 UV 存入 Redis Set（用于去重统计 UV）
            stringRedisTemplate.opsForSet().add(SHORT_LINK_STATS_UV_KEY + fullShortUrl, uv.get());
        };

        // 如果请求中存在 Cookie 数组
        if (ArrayUtil.isNotEmpty(cookies)) {
            // 从 Cookie 中查找名为 "uv" 的 Cookie
            Arrays.stream(cookies)
                    .filter(each -> Objects.equals(each.getName(), "uv"))
                    .findFirst()
                    .map(Cookie::getValue)
                    .ifPresentOrElse(
                            // 如果存在 uv Cookie，则使用其值
                            each -> {
                                uv.set(each);
                                // 尝试将 UV 加入 Redis Set，返回值表示新增成功的元素数量
                                Long uvAdded = stringRedisTemplate.opsForSet().add(
                                        SHORT_LINK_STATS_UV_KEY + fullShortUrl,
                                        each);
                                // 如果返回值 >0，说明该 UV 是首次出现（之前 Set 中不存在）
                                uvFirstFlag.set(uvAdded != null && uvAdded > 0L);
                            },
                            // 如果不存在 uv Cookie，则执行新增任务（生成新 UV）
                            addResponseCookieTask);
        } else {
            // 如果没有 Cookie，则直接生成新 UV
            addResponseCookieTask.run();
        }

        // 获取客户端真实 IP（考虑代理等情况）
        String remoteAddr = LinkUtil.getActualIp(((HttpServletRequest) request));
        // 获取操作系统类型（如 Windows、macOS、Android 等）
        String os = LinkUtil.getOs(((HttpServletRequest) request));
        // 获取浏览器类型（如 Chrome、Firefox 等）
        String browser = LinkUtil.getBrowser(((HttpServletRequest) request));
        // 获取设备类型（如 PC、Mobile、Tablet）
        String device = LinkUtil.getDevice(((HttpServletRequest) request));
        // 获取网络类型（如 WiFi、4G、5G 等）
        String network = LinkUtil.getNetwork(((HttpServletRequest) request));

        // 记录独立 IP（UIP）：将客户端 IP 存入 Redis Set，并判断是否首次出现
        Long uipAdded = stringRedisTemplate.opsForSet().add(SHORT_LINK_STATS_UIP_KEY + fullShortUrl, remoteAddr);
        boolean uipFirstFlag = uipAdded != null && uipAdded > 0L;

        // 构建并返回统计记录 DTO
        return ShortLinkStatsRecordDTO.builder()
                .fullShortUrl(fullShortUrl) // 完整短链接
                .uv(uv.get()) // 用户唯一标识
                .uvFirstFlag(uvFirstFlag.get()) // 是否首次 UV
                .uipFirstFlag(uipFirstFlag) // 是否首次 IP
                .remoteAddr(remoteAddr) // 客户端 IP
                .os(os) // 操作系统
                .browser(browser) // 浏览器
                .device(device) // 设备类型
                .network(network) // 网络类型
                .currentDate(new Date()) // 当前时间（统计日期）
                .build();
    }

    @Override
    public void shortLinkStats(ShortLinkStatsRecordDTO statsRecord) {
        Map<String, String> producerMap = new HashMap<>();
        producerMap.put("statsRecord", JSON.toJSONString(statsRecord));
        // 跳转统计不在主链路同步落库，而是投递到 Redis Stream 异步消费。
        shortLinkStatsSaveProducer.send(producerMap);
    }

    private String generateSuffix(ShortLinkCreateReqDTO requestParam) {
        int customGenerateCount = 0;
        String shorUri;
        while (true) {
            if (customGenerateCount > 10) {
                throw new ServiceException("短链接频繁生成，请稍后再试");
            }
            String originUrl = requestParam.getOriginUrl();
            originUrl += UUID.randomUUID().toString();
            // 短链接哈希算法生成冲突问题如何解决？详情查看：https://nageoffer.com/shortlink/question
            shorUri = HashUtil.hashToBase62(originUrl);
            // 判断短链接是否存在为什么不使用Set结构？详情查看：https://nageoffer.com/shortlink/question
            // 如果布隆过滤器挂了，里边存的数据全丢失了，怎么恢复呢？详情查看：https://nageoffer.com/shortlink/question
            if (!shortUriCreateCachePenetrationBloomFilter.contains(createShortLinkDefaultDomain + "/" + shorUri)) {
                break;
            }
            customGenerateCount++;
        }
        return shorUri;
    }

    private String generateSuffixByLock(ShortLinkCreateReqDTO requestParam) {
        int customGenerateCount = 0;
        String shorUri;
        while (true) {
            if (customGenerateCount > 10) {
                throw new ServiceException("短链接频繁生成，请稍后再试");
            }
            String originUrl = requestParam.getOriginUrl();
            originUrl += UUID.randomUUID().toString();
            // 短链接哈希算法生成冲突问题如何解决？详情查看：https://nageoffer.com/shortlink/question
            shorUri = HashUtil.hashToBase62(originUrl);
            LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                    .eq(ShortLinkDO::getGid, requestParam.getGid())
                    .eq(ShortLinkDO::getFullShortUrl, createShortLinkDefaultDomain + "/" + shorUri)
                    .eq(ShortLinkDO::getDelFlag, 0);
            ShortLinkDO shortLinkDO = baseMapper.selectOne(queryWrapper);
            if (shortLinkDO == null) {
                break;
            }
            customGenerateCount++;
        }
        return shorUri;
    }

    @SneakyThrows
    private String getFavicon(String url) {
        URL targetUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) targetUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        int responseCode = connection.getResponseCode();
        if (HttpURLConnection.HTTP_OK == responseCode) {
            Document document = Jsoup.connect(url).get();
            Element faviconLink = document.select("link[rel~=(?i)^(shortcut )?icon]").first();
            if (faviconLink != null) {
                return faviconLink.attr("abs:href");
            }
        }
        return null;
    }

    private void verificationWhitelist(String originUrl) {
        Boolean enable = gotoDomainWhiteListConfiguration.getEnable();
        if (enable == null || !enable) {
            return;
        }
        String domain = LinkUtil.extractDomain(originUrl);
        if (StrUtil.isBlank(domain)) {
            throw new ClientException("跳转链接填写错误");
        }
        List<String> details = gotoDomainWhiteListConfiguration.getDetails();
        if (!details.contains(domain)) {
            throw new ClientException("演示环境为避免恶意攻击，请生成以下网站跳转链接：" + gotoDomainWhiteListConfiguration.getNames());
        }
    }
}
