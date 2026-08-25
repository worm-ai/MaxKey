/*
 * Copyright [2026] [MaxKey of copyright http://www.maxkey.top]
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

package org.dromara.maxkey.notification.core.event;

/**
 * 主体类型。
 *
 * <p>与下游权限平台的 subjectType 取值对齐：下游按 {@link #wireName()} 识别。
 *
 * @author Avatar-Knowledge
 */
public enum SubjectChangeType {

    /** 用户。 */
    USER("user"),

    /** 用户组。 */
    GROUP("group"),

    /** 角色。 */
    ROLE("role"),

    /** 组织机构。 */
    ORGANIZATION("organization");

    private final String wireName;

    SubjectChangeType(String wireName) {
        this.wireName = wireName;
    }

    /**
     * 下游协议中使用的类型名（小写）。
     *
     * @return 协议类型名
     */
    public String wireName() {
        return wireName;
    }
}
