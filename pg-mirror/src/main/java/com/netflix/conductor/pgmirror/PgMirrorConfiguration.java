/*
 * Copyright 2026 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.pgmirror;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.netflix.conductor.dao.ExecutionDAO;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Wires the mirror around the persistence's {@link ExecutionDAO} from the outside, so the
 * persistence module (Scylla) is not changed. Only active with {@code
 * conductor.pg-mirror.enabled=true} and a persistence other than postgres (the mirror writes
 * towards Postgres, so it makes no sense on it).
 */
@Configuration
@EnableConfigurationProperties(PgMirrorProperties.class)
@ConditionalOnExpression(
        "${conductor.pg-mirror.enabled:false} and '${conductor.db.type:}' != 'postgres'")
public class PgMirrorConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgMirrorConfiguration.class);

    @Bean(initMethod = "start", destroyMethod = "close")
    public RedisPgMirrorPublisher pgMirrorPublisher(PgMirrorProperties props, Environment env) {
        String key = props.getQueueKey();
        if (key == null || key.isBlank()) {
            key = "pg-mirror:" + env.getProperty("conductor.app.stack", "default") + ":sql";
        }
        LOGGER.info(
                "pg-mirror enabled: Redis {}:{}, list {}",
                props.getRedis().getHost(),
                props.getRedis().getPort(),
                key);
        return new RedisPgMirrorPublisher(
                RedisListClient.jedis(props.getRedis(), props.getTimeoutMs()),
                key,
                props,
                System::currentTimeMillis);
    }

    /** Static: a BeanPostProcessor must exist before the beans it wraps. */
    @Bean
    public static BeanPostProcessor pgMirrorBeanPostProcessor() {
        return new Wrapper();
    }

    static class Wrapper implements BeanPostProcessor, BeanFactoryAware {

        private BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!(bean instanceof ExecutionDAO) || bean instanceof Advised) {
                return bean;
            }
            if (bean.getClass().getName().contains("Postgres")) {
                LOGGER.error(
                        "pg-mirror: {} is a Postgres DAO; the mirror writes towards Postgres and is not applied to it",
                        beanName);
                return bean;
            }
            ProxyFactory factory = new ProxyFactory(bean); // proxies every interface of the DAO
            factory.addAdvice(
                    new PgMirrorExecutionDaoInterceptor(
                            (ExecutionDAO) bean,
                            new PgMirrorSql(beanFactory.getBean(ObjectMapper.class)),
                            beanFactory.getBean(PgMirrorPublisher.class)));
            LOGGER.info(
                    "pg-mirror: wrapped the execution DAO {} ({})",
                    beanName,
                    bean.getClass().getSimpleName());
            return factory.getProxy();
        }
    }
}
