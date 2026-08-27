/*
 * 
 */

package com.nageoffer.shortlink.project.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkDO;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.req.RecycleBinRecoverReqDTO;
import com.nageoffer.shortlink.project.dto.req.RecycleBinRemoveReqDTO;
import com.nageoffer.shortlink.project.dto.req.RecycleBinSaveReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkRecycleBinPageReqDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkPageRespDTO;
import com.nageoffer.shortlink.project.service.RecycleBinService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_IS_NULL_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_SHORT_LINK_KEY;

/**
 * 回收站管理接口实现层
 * 负责短链接的移入回收站、恢复、彻底删除以及分页查询回收站列表。
 * 回收站功能通过修改 ShortLinkDO 的 enableStatus 字段（0-启用，1-回收站）实现软删除。
 * 
 * 缓存处理原则：
 * - 移入回收站：删除正向缓存（GOTO_SHORT_LINK_KEY），用户点击将无法跳转，显示404。
 * - 恢复：删除空值缓存（GOTO_IS_NULL_SHORT_LINK_KEY），因为之前可能因过期等原因缓存了空值，
 *   恢复后链接有效，需清除空值缓存以保证跳转正常。
 * - 彻底删除：无需操作缓存，因为该链接已不可用，且之前已删除正向缓存。
 * 

 */
@Service
@RequiredArgsConstructor
public class RecycleBinServiceImpl extends ServiceImpl<ShortLinkMapper, ShortLinkDO> implements RecycleBinService {

    // Redis 操作模板，用于缓存管理
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 将短链接移入回收站（软删除）
     * 
     * 业务逻辑：
     * 1. 根据 gid 和 fullShortUrl 定位到具体记录（必须处于启用状态、未删除）。
     * 2. 将 enableStatus 更新为 1（回收站状态），即软删除。
     * 3. 删除该短链接的 Redis 正向缓存（GOTO_SHORT_LINK_KEY），
     *    使用户后续点击无法命中缓存，从而无法跳转（跳转链路中会查数据库检查 enable_status 或返回404）。
     * 
     * 注意：此处不删除空值缓存（GOTO_IS_NULL_SHORT_LINK_KEY），因为该链接本身是存在的，
     * 只是被移入回收站，空值缓存是针对不存在的链接，不影响。
     * 
     * @param requestParam 包含 gid 和 fullShortUrl 的请求参数
     */
    @Override
    public void saveRecycleBin(RecycleBinSaveReqDTO requestParam) {
        // 构造更新条件：根据 gid 和 fullShortUrl 精确定位，且必须为启用状态（enableStatus=0）且未被删除（delFlag=0）
        LambdaUpdateWrapper<ShortLinkDO> updateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                .eq(ShortLinkDO::getGid, requestParam.getGid())
                .eq(ShortLinkDO::getEnableStatus, 0)   // 只允许将启用的链接移入回收站
                .eq(ShortLinkDO::getDelFlag, 0);       // 确保记录未被彻底删除

        // 只更新 enableStatus 字段，设置为 1（回收站状态）
        ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                .enableStatus(1)
                .build();
        baseMapper.update(shortLinkDO, updateWrapper);

        // 删除正向缓存（短链接 -> 长链接的映射），使跳转链路无法通过缓存获取原始链接
        stringRedisTemplate.delete(String.format(GOTO_SHORT_LINK_KEY, requestParam.getFullShortUrl()));
    }

    /**
     * 分页查询回收站中的短链接列表
     * 
     * 业务逻辑：
     * 1. 调用自定义分页方法 baseMapper.pageRecycleBinLink()，该方法会查询 enableStatus=1 且 delFlag=0 的记录。
     * 2. 将查询结果转换为响应 DTO，并对 domain 字段添加 "http://" 前缀，供前端展示完整链接。
     * 
     * @param requestParam 分页参数（页码、每页大小、分组ID等）
     * @return 分页结果，包含 ShortLinkPageRespDTO 列表
     */
    @Override
    public IPage<ShortLinkPageRespDTO> pageShortLink(ShortLinkRecycleBinPageReqDTO requestParam) {
        // 执行分页查询（由 Mapper 实现，可能包含多表关联或特定过滤条件）
        IPage<ShortLinkDO> resultPage = baseMapper.pageRecycleBinLink(requestParam);

        // 将实体对象转换为响应 DTO，并补全域名前缀
        return resultPage.convert(each -> {
            ShortLinkPageRespDTO result = BeanUtil.toBean(each, ShortLinkPageRespDTO.class);
            result.setDomain("http://" + result.getDomain());  // 统一添加协议头
            return result;
        });
    }

    /**
     * 从回收站恢复短链接（撤销软删除）
     * 
     * 业务逻辑：
     * 1. 根据 gid 和 fullShortUrl 定位回收站中的记录（必须处于回收站状态 enableStatus=1 且未彻底删除）。
     * 2. 将 enableStatus 更新为 0（启用状态）。
     * 3. 删除该短链接的空值缓存（GOTO_IS_NULL_SHORT_LINK_KEY）。
     *    原因：在链接被移入回收站或过期期间，跳转链路可能已缓存了空值标记（-），
     *    现在链接恢复有效，必须清除该空值缓存，否则用户点击仍会返回404。
     * 
     * 注意：恢复操作不会立即重建正向缓存（GOTO_SHORT_LINK_KEY），
     * 而是由下一次用户访问时通过 Cache Aside 模式触发回填，以保证缓存与数据库最终一致。
     * 
     * @param requestParam 包含 gid 和 fullShortUrl 的请求参数
     */
    @Override
    public void recoverRecycleBin(RecycleBinRecoverReqDTO requestParam) {
        // 构造更新条件：必须处于回收站状态（enableStatus=1）且未被彻底删除
        LambdaUpdateWrapper<ShortLinkDO> updateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                .eq(ShortLinkDO::getGid, requestParam.getGid())
                .eq(ShortLinkDO::getEnableStatus, 1)   // 只允许从回收站中恢复
                .eq(ShortLinkDO::getDelFlag, 0);       // 未彻底删除

        // 将 enableStatus 置为 0（启用）
        ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                .enableStatus(0)
                .build();
        baseMapper.update(shortLinkDO, updateWrapper);

        // 删除空值缓存（可能因之前无效而缓存了空值标记）
        stringRedisTemplate.delete(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, requestParam.getFullShortUrl()));
        // 注意：正向缓存不会立即重建，待用户访问时由 restoreUrl 方法回填
    }

    /**
     * 彻底删除回收站中的短链接（物理删除/逻辑删除）
     * 
     * 业务逻辑：
     * 1. 根据 gid 和 fullShortUrl 定位回收站中的记录（必须处于回收站状态、未被删除且删除时间为0）。
     * 2. 设置 delFlag=1（逻辑删除），并记录当前时间戳到 delTime。
     *    （如果是分库分表环境，物理删除可能影响数据恢复，故采用逻辑删除保留历史数据）
     * 3. 不操作任何缓存，因为该链接已在回收站中，正向缓存已被删除（移入时已删），
     *    且彻底删除后该链接将不再可用，无需额外清理。
     * 
     * 注意：此操作不可逆，数据仅保留在数据库中作为历史记录（delFlag=1）。
     * 若需物理删除，可在 Mapper 中实现硬删除，但本项目采用逻辑删除以便数据恢复审计。
     * 
     * @param requestParam 包含 gid 和 fullShortUrl 的请求参数
     */
    @Override
    public void removeRecycleBin(RecycleBinRemoveReqDTO requestParam) {
        // 构造更新条件：必须处于回收站状态（enableStatus=1）、未删除且删除时间为0（表示未被逻辑删除过）
        LambdaUpdateWrapper<ShortLinkDO> updateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                .eq(ShortLinkDO::getGid, requestParam.getGid())
                .eq(ShortLinkDO::getEnableStatus, 1)   // 仅允许从回收站中彻底删除
                .eq(ShortLinkDO::getDelTime, 0L)       // 确保记录尚未被逻辑删除
                .eq(ShortLinkDO::getDelFlag, 0);       // 确保未标记删除

        // 构建更新对象：设置删除标记和删除时间
        ShortLinkDO delShortLinkDO = ShortLinkDO.builder()
                .delTime(System.currentTimeMillis())
                .build();
        delShortLinkDO.setDelFlag(1);  // 逻辑删除标记

        // 执行更新（将 delFlag 置为1，并记录删除时间）
        baseMapper.update(delShortLinkDO, updateWrapper);

        // 不操作缓存：正向缓存已在移入回收站时删除，空值缓存针对的是不存在的链接，彻底删除后无需处理。
        // 如果之前有正向缓存残留（正常情况下不会），也不会影响，因为链接已不可用且不会再有跳转请求。
    }
}