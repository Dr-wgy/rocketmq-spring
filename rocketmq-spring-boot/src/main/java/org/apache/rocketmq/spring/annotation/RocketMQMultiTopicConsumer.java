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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Multi-topic consumer annotation for class level.
 * Allows a single consumer group to consume multiple topics with different configurations.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RocketMQMultiTopicConsumer {

    String NAME_SERVER_PLACEHOLDER = "${rocketmq.name-server:}";
    String ACCESS_KEY_PLACEHOLDER = "${rocketmq.consumer.access-key:}";
    String SECRET_KEY_PLACEHOLDER = "${rocketmq.consumer.secret-key:}";
    String TRACE_TOPIC_PLACEHOLDER = "${rocketmq.consumer.customized-trace-topic:}";
    String ACCESS_CHANNEL_PLACEHOLDER = "${rocketmq.access-channel:}";

    /**
     * Consumer group name. It's required and needs to be globally unique.
     * See <a href="https://rocketmq.apache.org/docs/domainModel/07consumergroup">here</a> for further discussion.
     */
    String consumerGroup();

    /**
     * Control consume mode, you can choice receive message concurrently or orderly.
     */
    ConsumeMode consumeMode() default ConsumeMode.CONCURRENTLY;

    /**
     * Control message mode, if you want all subscribers receive message all message, broadcasting is a good choice.
     */
    MessageModel messageModel() default MessageModel.CLUSTERING;

    /**
     * Max consumer thread number.
     */
    int consumeThreadMax() default 64;

    /**
     * Consumer thread number.
     */
    int consumeThreadNumber() default 20;

    /**
     * Max re-consume times.
     */
    int maxReconsumeTimes() default -1;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    long consumeTimeout() default 15L;

    /**
     * The property of "access-key".
     */
    String accessKey() default ACCESS_KEY_PLACEHOLDER;

    /**
     * The property of "secret-key".
     */
    String secretKey() default SECRET_KEY_PLACEHOLDER;

    /**
     * Switch flag instance for message trace.
     */
    boolean enableMsgTrace() default false;

    /**
     * The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    String customizedTraceTopic() default TRACE_TOPIC_PLACEHOLDER;

    /**
     * The property of "name-server".
     */
    String nameServer() default NAME_SERVER_PLACEHOLDER;

    /**
     * The property of "access-channel".
     */
    String accessChannel() default ACCESS_CHANNEL_PLACEHOLDER;

    /**
     * The property of "tlsEnable" default false.
     */
    String tlsEnable() default "false";

    /**
     * The namespace of consumer.
     */
    String namespace() default "";

    /**
     * The namespace V2 version of listener, it can not be used in combination with namespace.
     */
    String namespaceV2() default "";

    /**
     * Message consume retry strategy in concurrently mode.
     *
     * -1,no retry,put into DLQ directly
     * 0,broker control retry frequency
     * >0,client control retry frequency
     */
    int delayLevelWhenNextConsume() default 0;

    /**
     * The interval of suspending the pull in orderly mode, in milliseconds.
     *
     * The minimum value is 10 and the maximum is 30000.
     */
    int suspendCurrentQueueTimeMillis() default 1000;

    /**
     * Maximum time to await message consuming when shutdown consumer, in milliseconds.
     * The minimum value is 0
     */
    int awaitTerminationMillisWhenShutdown() default 1000;

    /**
     * The property of "instanceName".
     */
    String instanceName() default "DEFAULT";
}