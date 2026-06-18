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
package com.netflix.conductor.tracing;

import java.util.Set;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Aspect
@Component
@ConditionalOnProperty(name = "conductor.tracing.manual.enabled", havingValue = "true")
public class BusinessTracingAspect {

    /**
     * v1 production scope: only workflow start API spans. Execution-path spans (decide, enqueue)
     * are created by {@link WorkflowExecutionTracing}.
     */
    static final Set<String> ALLOWED_OPERATIONS = Set.of("workflow.start");

    private final TracingFacade tracing;

    public BusinessTracingAspect(TracingFacade tracing) {
        this.tracing = tracing;
    }

    @Around("@annotation(businessTrace)")
    public Object traceBusinessMethod(ProceedingJoinPoint joinPoint, BusinessTrace businessTrace)
            throws Throwable {
        String operation = businessTrace.value();
        if (!ALLOWED_OPERATIONS.contains(operation)) {
            return joinPoint.proceed();
        }
        InvocationState state = new InvocationState();

        tracing.run(
                operation,
                () -> {
                    BusinessTraceTags.tagBefore(tracing, operation, joinPoint);
                    try {
                        state.result = joinPoint.proceed();
                    } catch (Throwable t) {
                        state.error = t;
                    }
                    if (state.error == null) {
                        BusinessTraceTags.tagAfter(tracing, operation, joinPoint, state.result);
                    }
                });

        if (state.error != null) {
            throw state.error;
        }
        return state.result;
    }

    private static final class InvocationState {
        private Object result;
        private Throwable error;
    }
}
