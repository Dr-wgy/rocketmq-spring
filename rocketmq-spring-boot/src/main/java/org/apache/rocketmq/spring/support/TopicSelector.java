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

import org.apache.rocketmq.spring.annotation.SelectorType;

/**
 * Topic and selector configuration for multi-topic consumers.
 */
public class TopicSelector {
    private String topic;
    private SelectorType selectorType;
    private String selectorExpression;

    public TopicSelector(String topic, SelectorType selectorType, String selectorExpression) {
        this.topic = topic;
        this.selectorType = selectorType;
        this.selectorExpression = selectorExpression;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public SelectorType getSelectorType() {
        return selectorType;
    }

    public void setSelectorType(SelectorType selectorType) {
        this.selectorType = selectorType;
    }

    public String getSelectorExpression() {
        return selectorExpression;
    }

    public void setSelectorExpression(String selectorExpression) {
        this.selectorExpression = selectorExpression;
    }

    @Override
    public String toString() {
        return "TopicSelector{" +
            "topic='" + topic + '\'' +
            ", selectorType=" + selectorType +
            ", selectorExpression='" + selectorExpression + '\'' +
            '}';
    }
}