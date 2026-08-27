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

package com.nageoffer.shortlink.project.config;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Sentinel;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 初始化限流配置
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@Component
public class SentinelRuleConfig implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {
        List<FlowRule> rules = new ArrayList<>();
        FlowRule createOrderRule = new FlowRule();
        // 为短链接创建接口配置 QPS 流控规则，阈值可按环境调整。
        //对应创建短链接接口上面的这个 @SentinelResource("create_short-link")
        createOrderRule.setResource("create_short-link");
        //限流维度。QPS 表示按每秒查询数（即每秒请求次数）来统计，而不是按并发线程数。
        createOrderRule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        //阈值门槛。QPS 限制为 1，即每秒只允许 1 个请求通过。第 2 个请求在同一秒内到达，就会触发限流。
        createOrderRule.setCount(1);
        rules.add(createOrderRule);
       // 生效动作。将上面构建的规则列表加载进 Sentinel 的核心处理器，使规则正式生效。
        FlowRuleManager.loadRules(rules);
    }
}
