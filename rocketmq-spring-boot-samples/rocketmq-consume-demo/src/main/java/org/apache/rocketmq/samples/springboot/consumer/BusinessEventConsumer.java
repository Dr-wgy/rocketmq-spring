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

package org.apache.rocketmq.samples.springboot.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMultiTopicConsumer;
import org.apache.rocketmq.spring.annotation.RocketMQTopicHandler;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Example of multi-topic consumer that handles different business events
 * in a single consumer group.
 */
@Service
@RocketMQMultiTopicConsumer(
    consumerGroup = "business-event-consumer-group",
    consumeMode = ConsumeMode.CONCURRENTLY,
    consumeThreadMax = 32,
    consumeThreadNumber = 16
)
public class BusinessEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BusinessEventConsumer.class);

    /**
     * Handle user creation and update events
     */
    @RocketMQTopicHandler(
        topic = "user-events",
        tags = "CREATE||UPDATE",
        description = "Handle user creation and update events"
    )
    public void handleUserEvents(UserEvent event, MessageExt messageExt) {
        String tag = messageExt.getTags();
        logger.info("Received user event: tag={}, userId={}, event={}",
                   tag, event.getUserId(), event);

        if ("CREATE".equals(tag)) {
            handleUserCreate(event);
        } else if ("UPDATE".equals(tag)) {
            handleUserUpdate(event);
        }
    }

    /**
     * Handle user deletion events separately
     */
    @RocketMQTopicHandler(
        topic = "user-events",
        tags = "DELETE"
    )
    public void handleUserDeletion(UserEvent event) {
        logger.info("Processing user deletion: userId={}", event.getUserId());
        // Handle user deletion logic
        deleteUserData(event.getUserId());
    }

    /**
     * Handle all order events
     */
    @RocketMQTopicHandler(
        topic = "order-events",
        tags = "*"
    )
    public void handleOrderEvents(String message, MessageExt messageExt) {
        logger.info("Received order event: tags={}, message={}",
                   messageExt.getTags(), message);

        OrderEvent orderEvent = JSON.parseObject(message, OrderEvent.class);
        processOrder(orderEvent);
    }

    /**
     * Handle payment events using SQL92 filter
     */
    @RocketMQTopicHandler(
        topic = "payment-events",
        selectorType = SelectorType.SQL92,
        sqlExpression = "status IN ('SUCCESS', 'FAILED') AND amount > 100"
    )
    public void handlePaymentEvents(PaymentEvent event, MessageExt messageExt) {
        String status = messageExt.getUserProperty("status");
        logger.info("Processing payment event: orderId={}, status={}, amount={}",
                   event.getOrderId(), status, event.getAmount());

        if ("SUCCESS".equals(status)) {
            handlePaymentSuccess(event);
        } else if ("FAILED".equals(status)) {
            handlePaymentFailed(event);
        }
    }

    /**
     * Handle VIP user events with specific tags
     */
    @RocketMQTopicHandler(
        topic = "vip-events",
        tags = "UPGRADE||DOWNGRADE"
    )
    public void handleVipEvents(byte[] messageBody, MessageExt messageExt) {
        String message = new String(messageBody);
        String tag = messageExt.getTags();

        logger.info("Processing VIP event: tag={}, message={}", tag, message);

        VipEvent vipEvent = JSON.parseObject(message, VipEvent.class);

        if ("UPGRADE".equals(tag)) {
            handleVipUpgrade(vipEvent);
        } else if ("DOWNGRADE".equals(tag)) {
            handleVipDowngrade(vipEvent);
        }
    }

    /**
     * Handle notification events (all tags)
     */
    @RocketMQTopicHandler(
        topic = "notification-events"
    )
    public void handleNotificationEvents(NotificationEvent event) {
        logger.info("Sending notification: type={}, userId={}",
                   event.getType(), event.getUserId());
        sendNotification(event);
    }

    // Business logic methods
    private void handleUserCreate(UserEvent event) {
        logger.debug("Creating user profile for: {}", event.getUserId());
        // Implementation for user creation
    }

    private void handleUserUpdate(UserEvent event) {
        logger.debug("Updating user profile for: {}", event.getUserId());
        // Implementation for user update
    }

    private void deleteUserData(String userId) {
        logger.debug("Deleting user data for: {}", userId);
        // Implementation for user deletion
    }

    private void processOrder(OrderEvent event) {
        logger.debug("Processing order: {}", event.getOrderId());
        // Implementation for order processing
    }

    private void handlePaymentSuccess(PaymentEvent event) {
        logger.debug("Payment successful for order: {}", event.getOrderId());
        // Implementation for successful payment
    }

    private void handlePaymentFailed(PaymentEvent event) {
        logger.debug("Payment failed for order: {}", event.getOrderId());
        // Implementation for failed payment
    }

    private void handleVipUpgrade(VipEvent event) {
        logger.debug("Processing VIP upgrade for user: {}", event.getUserId());
        // Implementation for VIP upgrade
    }

    private void handleVipDowngrade(VipEvent event) {
        logger.debug("Processing VIP downgrade for user: {}", event.getUserId());
        // Implementation for VIP downgrade
    }

    private void sendNotification(NotificationEvent event) {
        logger.debug("Sending {} notification to user: {}",
                    event.getType(), event.getUserId());
        // Implementation for notification sending
    }

    // Event classes (would normally be in separate files)
    public static class UserEvent {
        private String userId;
        private String userName;
        private String email;

        // getters and setters
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getUserName() { return userName; }
        public void setUserName(String userName) { this.userName = userName; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }

        @Override
        public String toString() {
            return "UserEvent{userId='" + userId + "', userName='" + userName + "', email='" + email + "'}";
        }
    }

    public static class OrderEvent {
        private String orderId;
        private String userId;
        private double amount;

        // getters and setters
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    public static class PaymentEvent {
        private String orderId;
        private double amount;
        private String paymentMethod;

        // getters and setters
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
        public String getPaymentMethod() { return paymentMethod; }
        public void setPaymentMethod(String paymentMethod) { this.paymentMethod = paymentMethod; }
    }

    public static class VipEvent {
        private String userId;
        private String vipLevel;

        // getters and setters
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getVipLevel() { return vipLevel; }
        public void setVipLevel(String vipLevel) { this.vipLevel = vipLevel; }
    }

    public static class NotificationEvent {
        private String userId;
        private String type;
        private String content;

        // getters and setters
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
    }
}