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

package org.apache.rocketmq.spring.annotation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.spring.core.RocketMQMultiTopicListener;
import org.apache.rocketmq.spring.support.RocketMQMessageListenerContainerRegistrar;
import org.apache.rocketmq.spring.support.TopicHandlerInfo;
import org.apache.rocketmq.spring.support.TopicSelector;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

/**
 * Bean post processor for multi-topic RocketMQ consumers.
 * Similar to RocketMQMessageListenerBeanPostProcessor but for multi-topic consumers.
 * Reuses existing RocketMQMessageListenerContainerRegistrar completely.
 */
public class RocketMQMultiTopicConsumerProcessor extends RocketMQMessageListenerBeanPostProcessor {

    public RocketMQMultiTopicConsumerProcessor(
        List<AnnotationEnhancer> enhancers,
        ObjectProvider<RocketMQMessageListenerContainerRegistrar> registrarProvider) {
        super(enhancers, registrarProvider);
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        RocketMQMultiTopicConsumer annotation = targetClass.getAnnotation(RocketMQMultiTopicConsumer.class);

        if (annotation != null) {
            // Validate and scan topic handlers
            Map<String, TopicHandlerInfo> topicHandlers = scanAndValidateTopicHandlers(bean);

            // Create multi-topic listener adapter
            RocketMQMultiTopicListener multiTopicListener = new RocketMQMultiTopicListener(bean);

            // Convert TopicHandlerInfo to TopicSelector list
            List<TopicSelector> topicSelectors = convertToTopicSelectors(topicHandlers);

            // Create synthetic RocketMQMessageListener annotation
            RocketMQMessageListener syntheticAnnotation = createSyntheticAnnotation(annotation);

            // Apply enhancement logic
            RocketMQMessageListener enhancedAnnotation = enhance(targetClass, syntheticAnnotation);

            // Register using multi-topic registrar method
            registrarObjectProvider.ifAvailable(registrar ->
                registrar.registerMultiTopicContainer(beanName, multiTopicListener, enhancedAnnotation, topicSelectors));
        }

        return bean;
    }

    /**
     * Scan and validate @RocketMQTopicHandler methods in the bean.
     * Throws exception if no valid handlers found or if topic/tags are missing.
     */
    private Map<String, TopicHandlerInfo> scanAndValidateTopicHandlers(Object bean) {
        Map<String, TopicHandlerInfo> handlers = new LinkedHashMap<>();
        Class<?> clazz = AopUtils.getTargetClass(bean);

        for (Method method : clazz.getMethods()) {
            RocketMQTopicHandler annotation = method.getAnnotation(RocketMQTopicHandler.class);
            if (annotation != null) {
                // Validate topic
                if (!StringUtils.hasText(annotation.topic())) {
                    throw new IllegalArgumentException(
                        String.format("@RocketMQTopicHandler on method %s.%s must specify a topic",
                            clazz.getSimpleName(), method.getName()));
                }

                // Validate tags (only TAG selector type is supported)
                if (!StringUtils.hasText(annotation.tags())) {
                    throw new IllegalArgumentException(
                        String.format("@RocketMQTopicHandler on method %s.%s must specify tags",
                            clazz.getSimpleName(), method.getName()));
                }

                // SQL92 selector type is not supported yet
                if (annotation.selectorType() == SelectorType.SQL92) {
                    throw new IllegalArgumentException(
                        String.format("@RocketMQTopicHandler on method %s.%s: SQL92 selector type is not supported yet, only TAG mode is supported",
                            clazz.getSimpleName(), method.getName()));
                }

                TopicHandlerInfo info = new TopicHandlerInfo(method, annotation);
                String key = info.getHandlerKey();

                // Check for duplicate handlers
                if (handlers.containsKey(key)) {
                    throw new IllegalArgumentException(
                        String.format("Duplicate topic handler found: %s in %s", key, clazz.getSimpleName()));
                }

                handlers.put(key, info);
            }
        }

        // Validate that at least one handler exists
        if (handlers.isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Class %s annotated with @RocketMQMultiTopicConsumer must have at least one method annotated with @RocketMQTopicHandler",
                    clazz.getSimpleName()));
        }

        return handlers;
    }

    /**
     * Convert TopicHandlerInfo map to TopicSelector list.
     * Only supports TAG selector type.
     * Splits tags by "||" and aggregates by topic, then rejoins tags with "||".
     * Throws exception for duplicate topic+tag combinations.
     */
    private List<TopicSelector> convertToTopicSelectors(Map<String, TopicHandlerInfo> topicHandlers) {
        Map<String, Set<String>> topicTagsMap = new LinkedHashMap<>(); // topic -> tags list
        Map<String, String> uniqueTopicTags = new HashMap<>(); // topic+tag -> method name (for error reporting)

        // 第一步：收集所有的 topic 和 tags，并检查重复
        for (TopicHandlerInfo handler : topicHandlers.values()) {
            RocketMQTopicHandler handlerAnnotation = handler.getAnnotation();
            String topic = handlerAnnotation.topic();
            String tagsStr = handlerAnnotation.tags();

            // 按照 "||" 拆分 tags
            String[] tagArray;
            if ("*".equals(tagsStr)) {
                tagArray = new String[]{"*"};
            } else {
                tagArray = tagsStr.split("\\|\\|");
            }

            // 收集每个 tag 到对应的 topic
            for (String tag : tagArray) {
                String cleanTag = tag.trim();
                String topicTagKey = topic + "#" + cleanTag;

                // 检查是否已经存在相同的 topic+tag 组合
                if (uniqueTopicTags.containsKey(topicTagKey)) {
                    throw new IllegalArgumentException(
                        String.format("Duplicate topic+tag subscription found: topic=%s, tag=%s. " +
                            "Already handled by method: %s, conflicting method: %s",
                            topic, cleanTag, uniqueTopicTags.get(topicTagKey), handler.getMethod().getName()));
                }

                // 记录这个 topic+tag 组合
                uniqueTopicTags.put(topicTagKey, handler.getMethod().getName());

                // 添加到 topic 的 tags 列表中
                topicTagsMap.computeIfAbsent(topic, k -> new HashSet<>()).add(cleanTag);
            }
        }

        // 第二步：按照 topic 聚合 tags，用 "||" 拼接
        List<TopicSelector> topicSelectors = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : topicTagsMap.entrySet()) {
            String topic = entry.getKey();
            Set<String> tags = entry.getValue();

            // 将同一个 topic 的所有 tags 用 "||" 拼接
            String aggregatedTags = String.join("||", tags);

            // 创建 TopicSelector
            TopicSelector topicSelector = new TopicSelector(
                topic,
                SelectorType.TAG,
                aggregatedTags
            );

            topicSelectors.add(topicSelector);
        }

        return topicSelectors;
    }

    /**
     * Create a synthetic RocketMQMessageListener annotation for multi-topic consumer.
     * This creates a base annotation with common settings, topics will be handled separately.
     */
    private RocketMQMessageListener createSyntheticAnnotation(RocketMQMultiTopicConsumer multiTopicAnnotation) {
        Map<String, Object> attributes = new HashMap<>();

        // Copy all attributes from multi-topic annotation
        attributes.put("consumerGroup", multiTopicAnnotation.consumerGroup());
        attributes.put("topic", ""); // Will be overridden by topicSelectors
        attributes.put("selectorExpression", "*"); // Will be overridden by topicSelectors
        attributes.put("selectorType", SelectorType.TAG); // Will be overridden by topicSelectors

        // Copy other attributes
        attributes.put("consumeMode", multiTopicAnnotation.consumeMode());
        attributes.put("messageModel", multiTopicAnnotation.messageModel());
        attributes.put("consumeThreadMax", multiTopicAnnotation.consumeThreadMax());
        attributes.put("consumeThreadNumber", multiTopicAnnotation.consumeThreadNumber());
        attributes.put("maxReconsumeTimes", multiTopicAnnotation.maxReconsumeTimes());
        attributes.put("consumeTimeout", multiTopicAnnotation.consumeTimeout());
        attributes.put("replyTimeout", 3000); // Default value
        attributes.put("accessKey", multiTopicAnnotation.accessKey());
        attributes.put("secretKey", multiTopicAnnotation.secretKey());
        attributes.put("enableMsgTrace", multiTopicAnnotation.enableMsgTrace());
        attributes.put("customizedTraceTopic", multiTopicAnnotation.customizedTraceTopic());
        attributes.put("nameServer", multiTopicAnnotation.nameServer());
        attributes.put("accessChannel", multiTopicAnnotation.accessChannel());
        attributes.put("tlsEnable", multiTopicAnnotation.tlsEnable());
        attributes.put("namespace", multiTopicAnnotation.namespace());
        attributes.put("namespaceV2", multiTopicAnnotation.namespaceV2());
        attributes.put("delayLevelWhenNextConsume", multiTopicAnnotation.delayLevelWhenNextConsume());
        attributes.put("suspendCurrentQueueTimeMillis", multiTopicAnnotation.suspendCurrentQueueTimeMillis());
        attributes.put("awaitTerminationMillisWhenShutdown", multiTopicAnnotation.awaitTerminationMillisWhenShutdown());
        attributes.put("instanceName", multiTopicAnnotation.instanceName());

        return AnnotationUtils.synthesizeAnnotation(attributes, RocketMQMessageListener.class, null);
    }
}