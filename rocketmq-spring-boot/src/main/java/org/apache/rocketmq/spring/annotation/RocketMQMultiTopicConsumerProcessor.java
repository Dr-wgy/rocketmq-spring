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

import org.apache.rocketmq.spring.core.RocketMQMultiTopicListener;
import org.apache.rocketmq.spring.support.RocketMQMessageListenerContainerRegistrar;
import org.apache.rocketmq.spring.support.TopicHandlerInfo;
import org.apache.rocketmq.spring.support.TopicSelector;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.*;
import java.util.List;

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

                // Validate tags (only for TAG selector type)
                if (annotation.selectorType() == SelectorType.TAG && !StringUtils.hasText(annotation.tags())) {
                    throw new IllegalArgumentException(
                        String.format("@RocketMQTopicHandler on method %s.%s with TAG selector type must specify tags",
                                     clazz.getSimpleName(), method.getName()));
                }

                // Validate SQL expression (only for SQL92 selector type)
                if (annotation.selectorType() == SelectorType.SQL92 && !StringUtils.hasText(annotation.sqlExpression())) {
                    throw new IllegalArgumentException(
                        String.format("@RocketMQTopicHandler on method %s.%s with SQL92 selector type must specify sqlExpression",
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
     */
    private List<TopicSelector> convertToTopicSelectors(Map<String, TopicHandlerInfo> topicHandlers) {
        List<TopicSelector> topicSelectors = new ArrayList<>();

        for (TopicHandlerInfo handler : topicHandlers.values()) {
            RocketMQTopicHandler handlerAnnotation = handler.getAnnotation();
            String selectorExpression;

            if (handlerAnnotation.selectorType() == SelectorType.TAG) {
                selectorExpression = handlerAnnotation.tags();
            } else if (handlerAnnotation.selectorType() == SelectorType.SQL92) {
                selectorExpression = handlerAnnotation.sqlExpression();
            } else {
                selectorExpression = "*"; // Default
            }

            TopicSelector topicSelector = new TopicSelector(
                handlerAnnotation.topic(),
                handlerAnnotation.selectorType(),
                selectorExpression
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