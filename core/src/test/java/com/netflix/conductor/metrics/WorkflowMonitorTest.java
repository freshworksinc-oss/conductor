/*
 * Copyright 2022 Conductor Authors.
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
package com.netflix.conductor.metrics;

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;

import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.TaskMetricInfo;
import com.netflix.conductor.dao.WorkflowMetricInfo;
import com.netflix.conductor.service.MetadataService;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class WorkflowMonitorTest {

    @Mock private MetadataService metadataService;
    @Mock private QueueDAO queueDAO;
    @Mock private ExecutionDAOFacade executionDAOFacade;

    private WorkflowMonitor workflowMonitor;

    @Before
    public void beforeEach() {
        workflowMonitor =
                new WorkflowMonitor(metadataService, queueDAO, executionDAOFacade, 1000, Set.of());
    }

    @Test
    public void testReportMetricsQueriesPerWorkflowName() {
        when(metadataService.getWorkflowMetricInfo())
                .thenReturn(
                        List.of(
                                new WorkflowMetricInfo("workflow1", "owner1"),
                                new WorkflowMetricInfo("workflow2", "owner2")));
        when(metadataService.getTaskMetricInfo()).thenReturn(List.of());

        workflowMonitor.reportMetrics();

        verify(executionDAOFacade).getPendingWorkflowCount("workflow1");
        verify(executionDAOFacade).getPendingWorkflowCount("workflow2");
    }

    @Test
    public void testReportMetricsNeverLoadsFullDefinitions() {
        when(metadataService.getWorkflowMetricInfo()).thenReturn(List.of());
        when(metadataService.getTaskMetricInfo()).thenReturn(List.of());

        workflowMonitor.reportMetrics();

        verify(metadataService, never()).getWorkflowDefs();
        verify(metadataService, never()).getTaskDefs();
    }

    @Test
    public void testInProgressCountQueriedOnlyWhenConcurrencyLimited() {
        when(metadataService.getWorkflowMetricInfo()).thenReturn(List.of());
        when(metadataService.getTaskMetricInfo())
                .thenReturn(
                        List.of(
                                new TaskMetricInfo("limited", "owner", 5),
                                new TaskMetricInfo("unlimited", "owner", 0)));

        workflowMonitor.reportMetrics();

        // Queue depth for every task; the in-progress count only where it is published.
        verify(queueDAO).getSize("limited");
        verify(queueDAO).getSize("unlimited");
        verify(executionDAOFacade).getInProgressTaskCount("limited");
        verify(executionDAOFacade, never()).getInProgressTaskCount("unlimited");
    }

    @Test
    public void testRefreshHappensOncePerInterval() {
        when(metadataService.getWorkflowMetricInfo()).thenReturn(List.of());
        when(metadataService.getTaskMetricInfo()).thenReturn(List.of());

        workflowMonitor.reportMetrics();
        workflowMonitor.reportMetrics();

        // metadataRefreshInterval is 1000, so the second run reuses what the first one read.
        verify(metadataService, times(1)).getWorkflowMetricInfo();
        verify(metadataService, times(1)).getTaskMetricInfo();
    }

    @Test
    public void testNoMetricsWhenCatalogEmpty() {
        when(metadataService.getWorkflowMetricInfo()).thenReturn(List.of());
        when(metadataService.getTaskMetricInfo()).thenReturn(List.of());

        workflowMonitor.reportMetrics();

        verify(executionDAOFacade, never()).getPendingWorkflowCount(anyString());
    }
}
