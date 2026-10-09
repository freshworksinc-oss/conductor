/*
 * Copyright 2023 Conductor Authors.
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
package com.netflix.conductor.core.execution;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.execution.tasks.Human;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.listener.TaskStatusListener;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.service.ExecutionLockService;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_HUMAN;
import static com.netflix.conductor.model.TaskModel.Status.IN_PROGRESS;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HumanTaskScheduleListenerTest {

    private TaskStatusListener taskStatusListener;
    private ExecutionDAOFacade executionDAOFacade;
    private WorkflowExecutorOps workflowExecutor;

    @Before
    public void setUp() {
        taskStatusListener = mock(TaskStatusListener.class);
        executionDAOFacade = mock(ExecutionDAOFacade.class);
        QueueDAO queueDAO = mock(QueueDAO.class);
        MetadataDAO metadataDAO = mock(MetadataDAO.class);
        WorkflowStatusListener workflowStatusListener = mock(WorkflowStatusListener.class);
        ExecutionLockService executionLockService = mock(ExecutionLockService.class);
        ParametersUtils parametersUtils = mock(ParametersUtils.class);
        IDGenerator idGenerator = new IDGenerator();
        SystemTaskRegistry systemTaskRegistry = new SystemTaskRegistry(Set.of(new Human()));

        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.getActiveWorkerLastPollTimeout()).thenReturn(Duration.ofSeconds(100));

        workflowExecutor =
                new WorkflowExecutorOps(
                        mock(DeciderService.class),
                        metadataDAO,
                        queueDAO,
                        mock(MetadataMapperService.class),
                        workflowStatusListener,
                        taskStatusListener,
                        executionDAOFacade,
                        properties,
                        executionLockService,
                        systemTaskRegistry,
                        parametersUtils,
                        idGenerator);
    }

    @Test
    public void scheduleHumanTaskNotifiesInProgressEvenWhenMappedAsInProgress() {
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId("wf-human-1");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("human_wf");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);

        TaskModel humanTask = new TaskModel();
        humanTask.setTaskId("human-task-1");
        humanTask.setTaskType(TASK_TYPE_HUMAN);
        humanTask.setReferenceTaskName("human_ref");
        humanTask.setWorkflowInstanceId(workflow.getWorkflowId());
        humanTask.setStatus(IN_PROGRESS);
        humanTask.setStartTime(System.currentTimeMillis());
        humanTask.setInputData(new HashMap<>());

        when(executionDAOFacade.createTasks(any())).thenReturn(List.of(humanTask));

        workflowExecutor.scheduleTask(workflow, List.of(humanTask));

        verify(taskStatusListener).onTaskInProgressIfEnabled(humanTask);
    }
}
