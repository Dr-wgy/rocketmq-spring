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

package org.apache.rocketmq.spring.support;

import java.lang.reflect.Method;
import org.apache.rocketmq.spring.annotation.RocketMQTopicHandler;

/**
 * Topic handler information holder.
 */
public class TopicHandlerInfo {

    private final Method method;
    private final RocketMQTopicHandler annotation;
    private final Class<?>[] parameterTypes;
    private final String handlerKey;

    public TopicHandlerInfo(Method method, RocketMQTopicHandler annotation) {
        this.method = method;
        this.annotation = annotation;
        this.parameterTypes = method.getParameterTypes();
        this.handlerKey = generateHandlerKey(annotation);

        // Ensure method is accessible
        method.setAccessible(true);
    }

    private String generateHandlerKey(RocketMQTopicHandler annotation) {
        // Only TAG mode is supported
        return annotation.topic() + "#TAG:" + annotation.tags();
    }

    public Method getMethod() {
        return method;
    }

    public RocketMQTopicHandler getAnnotation() {
        return annotation;
    }

    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public String getHandlerKey() {
        return handlerKey;
    }

    public String getTopic() {
        return annotation.topic();
    }

    public String getTags() {
        return annotation.tags();
    }

    public String getSqlExpression() {
        return annotation.sqlExpression();
    }

    @Override
    public String toString() {
        return "TopicHandlerInfo{" +
            "topic='" + annotation.topic() + '\'' +
            ", tags='" + annotation.tags() + '\'' +
            ", method='" + method.getName() + '\'' +
            '}';
    }
}