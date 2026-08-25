/*
 * Copyright [2020] [MaxKey of copyright http://www.maxkey.top]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 

package org.dromara.maxkey.persistence.service.impl;

import java.util.List;

import org.dromara.maxkey.entity.idm.GroupMember;
import org.dromara.maxkey.entity.idm.Groups;
import org.dromara.maxkey.entity.idm.UserInfo;
import org.dromara.maxkey.persistence.mapper.GroupMemberMapper;
import org.dromara.maxkey.persistence.service.GroupMemberService;
import org.dromara.mybatis.jpa.entity.JpaPageResults;
import org.dromara.mybatis.jpa.service.impl.JpaServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class GroupMemberServiceImpl  extends JpaServiceImpl<GroupMemberMapper,GroupMember,String> implements GroupMemberService{
    static final  Logger _logger = LoggerFactory.getLogger(GroupMemberServiceImpl.class);

    /**
     * Spring 应用事件发布器（Spring 自带）。
     *
     * <p>所有 IDM 写操作通过此发布器发领域事件，由消费方（如 notification 模块）
     * 以 {@code @EventListener} 方式订阅。这是标准的解耦机制：
     * persistence 层不依赖 notification 模块、不依赖任何下游系统；
     * notification 模块存在则自动订阅、不存在则事件无人消费、对业务零影响。
     *
     * <p>未来上提 framework 层时，这套事件机制本身就是通用能力的一部分。
     */
    @Autowired
    private org.springframework.context.ApplicationEventPublisher eventPublisher;

    /**
     * 重写父类 insert：成功后发布领域事件。
     *
     * <p>加上 @Transactional 确保有事务上下文，
     * TransactionalEventListener(BEFORE_COMMIT) 才能在事务提交前把事件写入 outbox 表，
     * 实现"业务数据 + outbox 记录"原子提交。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean insert(GroupMember groupMember) {
        boolean success = super.insert(groupMember);
        if (success && groupMember != null && isUserMember(groupMember.getType())) {
            try {
                eventPublisher.publishEvent(new org.dromara.maxkey.persistence.event.IdmEntityChangeEvent(
                        this, "GROUP_MEMBER", groupMember.getMemberId(),
                        "UPDATE", groupMember.getInstId()));
            } catch (RuntimeException eventFailure) {
                // 事件发布失败绝不能影响业务写结果
                _logger.error("failed to publish group member insert event: memberId={}",
                        groupMember.getMemberId(), eventFailure);
            }
        }
        return success;
    }

    private static boolean isUserMember(String type) {
        return type == null || type.trim().isEmpty()
                || "USER".equalsIgnoreCase(type.trim())
                || "USER-DYNAMIC".equalsIgnoreCase(type.trim());
    }

    @Override
    public int addDynamicMember(Groups dynamicGroup) {
        return getMapper().addDynamicMember(dynamicGroup);
    }
    
    @Override
    public int deleteDynamicMember(Groups dynamicGroup) {
        return getMapper().deleteDynamicMember(dynamicGroup);
    }
    
    @Override
    public int deleteByGroupId(String groupId) {
        return getMapper().deleteByGroupId(groupId);
    }
    
    @Override
    public List<UserInfo> queryMemberByGroupId(String groupId){
        return getMapper().queryMemberByGroupId(groupId);
    }
    
    
    @Override
    public JpaPageResults<Groups> noMember(GroupMember entity) {
        entity.build();
        List<Groups> resultslist = null;
        try {
            resultslist = getMapper().noMember(entity);
        } catch (Exception e) {
            _logger.error("queryPageResults Exception " , e);
        }
        //当前页记录数
        Integer records = JpaPageResults.parseRecords(resultslist);
        //总页数
        Integer totalCount =fetchCount(entity, resultslist);
        return new JpaPageResults<Groups>(entity.getPageNumber(),entity.getPageSize(),records,totalCount,resultslist);
    }
    
}
