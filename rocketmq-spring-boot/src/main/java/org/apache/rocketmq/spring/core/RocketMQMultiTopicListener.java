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

package org.apache.rocketmq.spring.core;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQTopicHandler;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.support.TopicHandlerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Multi-topic RocketMQ listener that adapts multi-topic consumers to regular RocketMQListener interface.
 * This allows reusing the existing RocketMQMessageListenerContainerRegistrar infrastructure.
 */
public class RocketMQMultiTopicListener implements RocketMQListener<MessageExt> {

    private static final Logger log = LoggerFactory.getLogger(RocketMQMultiTopicListener.class);

    private final Object targetBean;
    private final Map<String, TopicHandlerInfo> handlerMap;

    public RocketMQMultiTopicListener(Object targetBean) {
        this.targetBean = targetBean;
        this.handlerMap = scanTopicHandlers(targetBean);
    }

    @Override
    public void onMessage(MessageExt messageExt) {
        try {
            // Get real topic (handle retry messages)
            String realTopic = getRealTopic(messageExt);
            String tags = messageExt.getTags();

            log.debug("Processing multi-topic message: topic={}, tags={}, msgId={}",
                     realTopic, tags, messageExt.getMsgId());

            TopicHandlerInfo handler = findHandler(realTopic, tags, messageExt);

            if (handler != null) {
                invokeHandler(handler, messageExt);
                log.debug("Message processed successfully: msgId={}", messageExt.getMsgId());
            } else {
                log.warn("No handler found for message: topic={}, tags={}, msgId={}",
                        realTopic, tags, messageExt.getMsgId());
            }

        } catch (Exception e) {
            log.error("Failed to process multi-topic message: msgId={}", messageExt.getMsgId(), e);
            throw new RuntimeException("Message processing failed", e);
        }
    }

    private Map<String, TopicHandlerInfo> scanTopicHandlers(Object bean) {
        Map<String, TopicHandlerInfo> handlers = new LinkedHashMap<>();
        Class<?> clazz = AopProxyUtils.ultimateTargetClass(bean);

        for (Method method : clazz.getMethods()) {
            RocketMQTopicHandler annotation = method.getAnnotation(RocketMQTopicHandler.class);
            if (annotation != null) {
                TopicHandlerInfo info = new TopicHandlerInfo(method, annotation);
                String key = info.getHandlerKey();

                if (handlers.containsKey(key)) {
                    throw new IllegalArgumentException(
                        "Duplicate topic handler found: " + key + " in " + clazz.getSimpleName());
                }

                handlers.put(key, info);
                log.debug("Scanned topic handler: {} -> {}.{}", key, clazz.getSimpleName(), method.getName());
            }
        }

        if (handlers.isEmpty()) {
            throw new IllegalArgumentException("No @RocketMQTopicHandler methods found in " + clazz.getSimpleName());
        }

        return handlers;
    }

    private String getRealTopic(MessageExt messageExt) {
        String topic = messageExt.getTopic();
        if (topic.startsWith("%RETRY%") || topic.startsWith("%DLQ%")) {
            String realTopic = messageExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC);
            if (realTopic != null) {
                return realTopic;
            }
            log.warn("Retry message missing real topic info: msgId={}", messageExt.getMsgId());
        }

        return topic;
    }

    private TopicHandlerInfo findHandler(String topic, String tags, MessageExt messageExt) {
        // 1. Try exact TAG match
        String tagKey = topic + "#TAG:" + tags;
        TopicHandlerInfo handler = handlerMap.get(tagKey);
        if (handler != null) {
            log.debug("Found exact tag handler: {}", tagKey);
            return handler;
        }

        // 2. Try wildcard TAG match
        String wildcardKey = topic + "#TAG:*";
        handler = handlerMap.get(wildcardKey);
        if (handler != null) {
            log.debug("Found wildcard tag handler: {}", wildcardKey);
            return handler;
        }

        // 3. Try SQL92 handlers for this topic
        for (Map.Entry<String, TopicHandlerInfo> entry : handlerMap.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(topic + "#SQL:")) {
                TopicHandlerInfo sqlHandler = entry.getValue();
                if (matchesSqlExpression(sqlHandler.getSqlExpression(), messageExt)) {
                    log.debug("Found SQL handler: {}", key);
                    return sqlHandler;
                }
            }
        }

        return null;
    }

    private boolean matchesSqlExpression(String sqlExpression, MessageExt messageExt) {
        // Simple implementation - in production, you might want to use RocketMQ's SQL parser
        try {
            if (sqlExpression.contains("status")) {
                String status = messageExt.getUserProperty("status");
                if (status != null) {
                    return sqlExpression.toLowerCase().contains(status.toLowerCase());
                }
            }
            return true; // Default to true for this example
        } catch (Exception e) {
            log.warn("Failed to evaluate SQL expression: {}", sqlExpression, e);
            return false;
        }
    }

    private void invokeHandler(TopicHandlerInfo handler, MessageExt messageExt) throws Exception {
        Method method = handler.getMethod();
        Class<?>[] parameterTypes = handler.getParameterTypes();
        Object[] args = new Object[parameterTypes.length];

        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> paramType = parameterTypes[i];

            if (MessageExt.class.isAssignableFrom(paramType)) {
                args[i] = messageExt;
            } else if (String.class.equals(paramType)) {
                args[i] = new String(messageExt.getBody(), StandardCharsets.UTF_8);
            } else if (byte[].class.equals(paramType)) {
                args[i] = messageExt.getBody();
            } else {
                // Try to deserialize as JSON
                String json = new String(messageExt.getBody(), StandardCharsets.UTF_8);
                try {
                    args[i] = JSON.parseObject(json, paramType);
                } catch (Exception e) {
                    log.warn("Failed to deserialize message body to {}: {}",
                            paramType.getSimpleName(), e.getMessage());
                    args[i] = null;
                }
            }
        }

        method.invoke(targetBean, args);
    }

    /**
     * Get all topics that this multi-topic listener handles.
     * This is used for subscription.
     */
    public Map<String, String> getTopicSubscriptions() {
        Map<String, String> subscriptions = new LinkedHashMap<>();

        for (TopicHandlerInfo handler : handlerMap.values()) {
            String topic = handler.getTopic();

            // For multi-topic scenario, we use wildcard subscription
            // and do the filtering in the listener
            subscriptions.put(topic, "*");
        }

        return subscriptions;
    }
}