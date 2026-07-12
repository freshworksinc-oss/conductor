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
package com.netflix.conductor.migrator.dest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.migrator.convert.ModelConverter;
import com.netflix.conductor.migrator.tree.WorkflowTree;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.postgres.dao.PostgresExecutionDAO;
import com.netflix.conductor.postgres.dao.PostgresIndexDAO;

/**
 * Writes exported workflows into the DESTINATION Postgres via the embedded Conductor DAOs (D2).
 *
 * <p>For each workflow (children-first): {@code createWorkflow} + {@code createTasks} on the
 * execution DAO, then {@code indexWorkflow}/{@code indexTask} on the index DAO — the execution DAO
 * does NOT touch the search index, so search on the destination would miss imports otherwise.
 *
 * <p>The import is dormant (D4): it does not touch Redis or the decider queue, so per F4+F6 the
 * destination engine ignores these copies until {@code verifyAndRepair} is called. Every write is
 * idempotent (F1/F2), so a crashed import can be blindly retried.
 */
@Component
public class DestWriter {

    private static final Logger log = LoggerFactory.getLogger(DestWriter.class);

    private final PostgresExecutionDAO executionDao;
    private final PostgresIndexDAO indexDao;

    public DestWriter(PostgresExecutionDAO destExecutionDao, PostgresIndexDAO destIndexDao) {
        this.executionDao = destExecutionDao;
        this.indexDao = destIndexDao;
    }

    /** Import (or re-import) the whole tree, children-first, preserving all ids and statuses. */
    public void importTree(WorkflowTree tree) {
        for (Workflow workflow : tree.getChildrenFirst()) {
            importWorkflow(workflow);
        }
    }

    private void importWorkflow(Workflow workflow) {
        WorkflowModel model = ModelConverter.toWorkflowModel(workflow);

        // Execution state. createWorkflow does a plain INSERT (it is NOT an upsert — re-importing
        // an existing id would violate the workflow PK), so choose create vs. update by existence.
        // This keeps the import idempotent (crash-retry, and the future delta-sync re-import).
        boolean exists = executionDao.getWorkflow(workflow.getWorkflowId(), false) != null;
        if (exists) {
            executionDao.updateWorkflow(model);
        } else {
            executionDao.createWorkflow(model);
        }

        if (!model.getTasks().isEmpty()) {
            // createTasks inserts task rows + scheduled/in-progress bookkeeping and skips tasks
            // already scheduled (idempotent, F2). Follow with updateTask so an existing task's
            // json_data converges to the latest export.
            executionDao.createTasks(model.getTasks());
            model.getTasks().forEach(executionDao::updateTask);
        }

        // Search index (separate DAO). Build summaries from the common model — they format the
        // timestamps as ISO-instant strings, which is what PostgresIndexDAO parses.
        indexDao.indexWorkflow(new WorkflowSummary(workflow));
        for (Task task : workflow.getTasks()) {
            indexDao.indexTask(new TaskSummary(task));
        }

        log.info(
                "Imported workflow {} (status={}, tasks={}) into destination",
                workflow.getWorkflowId(),
                workflow.getStatus(),
                workflow.getTasks().size());
    }
}
