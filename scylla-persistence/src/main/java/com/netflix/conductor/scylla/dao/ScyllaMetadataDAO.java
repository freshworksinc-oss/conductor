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
package com.netflix.conductor.scylla.dao;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.core.exception.ConflictException;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PaginatedMetadataDAO;
import com.netflix.conductor.dao.WorkflowDefIndexDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.scylla.config.ScyllaProperties;
import com.netflix.conductor.scylla.util.Statements;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.conductor.common.metadata.tasks.TaskDef.ONE_HOUR;
import static com.netflix.conductor.scylla.util.Constants.TASK_DEFINITION_KEY;
import static com.netflix.conductor.scylla.util.Constants.TASK_DEFS_KEY;
import static com.netflix.conductor.scylla.util.Constants.WORKFLOW_DEFINITION_KEY;
import static com.netflix.conductor.scylla.util.Constants.WORKFLOW_DEF_INDEX_KEY;
import static com.netflix.conductor.scylla.util.Constants.WORKFLOW_DEF_NAME_VERSION_KEY;

@Trace
public class ScyllaMetadataDAO extends ScyllaBaseDAO
        implements MetadataDAO, PaginatedMetadataDAO, WorkflowDefIndexDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaMetadataDAO.class);
    private static final String CLASS_NAME = ScyllaMetadataDAO.class.getSimpleName();
    private static final String INDEX_DELIMITER = "/";

    /** Definitions read at the same time when many are needed (a window, not all at once). */
    private static final int READ_CONCURRENCY = 32;

    private final WorkflowDefIndex defIndex;
    private final PreparedStatement selectLatestWorkflowDefByNameStatement;

    private final PreparedStatement insertWorkflowDefStatement;
    private final PreparedStatement insertWorkflowDefVersionIndexStatement;
    private final PreparedStatement insertTaskDefStatement;

    private final PreparedStatement selectWorkflowDefStatement;
    private final PreparedStatement selectAllWorkflowDefVersionsByNameStatement;
    private final PreparedStatement selectAllWorkflowDefsStatement;
    private final PreparedStatement selectTaskDefStatement;
    private final PreparedStatement selectAllTaskDefsStatement;

    private final PreparedStatement selectAllWorkflowDefsLatestVersionsStatement;
    private final PreparedStatement updateWorkflowDefStatement;

    private final PreparedStatement deleteWorkflowDefStatement;
    private final PreparedStatement deleteWorkflowDefIndexStatement;
    private final PreparedStatement deleteTaskDefStatement;

    public ScyllaMetadataDAO(
            Session session,
            ObjectMapper objectMapper,
            ScyllaProperties properties,
            Statements statements) {
        super(session, objectMapper, properties);

        this.insertWorkflowDefStatement =
                session.prepare(statements.getInsertWorkflowDefStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.insertWorkflowDefVersionIndexStatement =
                session.prepare(statements.getInsertWorkflowDefVersionIndexStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.insertTaskDefStatement =
                session.prepare(statements.getInsertTaskDefStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.selectWorkflowDefStatement =
                session.prepare(statements.getSelectWorkflowDefStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectAllWorkflowDefVersionsByNameStatement =
                session.prepare(statements.getSelectAllWorkflowDefVersionsByNameStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectAllWorkflowDefsStatement =
                session.prepare(statements.getSelectAllWorkflowDefsStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectTaskDefStatement =
                session.prepare(statements.getSelectTaskDefStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectAllTaskDefsStatement =
                session.prepare(statements.getSelectAllTaskDefsStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectAllWorkflowDefsLatestVersionsStatement =
                session.prepare(statements.getSelectAllWorkflowDefsLatestVersionsStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        this.updateWorkflowDefStatement =
                session.prepare(statements.getUpdateWorkflowDefStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.deleteWorkflowDefStatement =
                session.prepare(statements.getDeleteWorkflowDefStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.deleteWorkflowDefIndexStatement =
                session.prepare(statements.getDeleteWorkflowDefIndexStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.deleteTaskDefStatement =
                session.prepare(statements.getDeleteTaskDefStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.selectLatestWorkflowDefByNameStatement =
                session.prepare(statements.getSelectLatestWorkflowDefByNameStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.defIndex =
                new WorkflowDefIndex(
                        this::loadIndexEntries,
                        properties.getWorkflowDefIndexCacheTtl().toMillis(),
                        System::currentTimeMillis);
    }

    @Override
    public TaskDef createTaskDef(TaskDef taskDef) {
        return insertOrUpdateTaskDef(taskDef);
    }

    @Override
    public TaskDef updateTaskDef(TaskDef taskDef) {
        return insertOrUpdateTaskDef(taskDef);
    }

    @Override
    public TaskDef getTaskDef(String name) {
        return getTaskDefFromDB(name);
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        return getAllTaskDefsFromDB();
    }

    @Override
    public void removeTaskDef(String name) {
        try {
            recordCassandraDaoRequests("removeTaskDef");
            session.execute(deleteTaskDefStatement.bind(name));
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "removeTaskDef");
            String errorMsg = String.format("Failed to remove task definition: %s", name);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @Override
    public void createWorkflowDef(WorkflowDef workflowDef) {
        try {
            String workflowDefinition = toJson(workflowDef);
            if (!session.execute(
                            insertWorkflowDefStatement.bind(
                                    workflowDef.getName(),
                                    workflowDef.getVersion(),
                                    workflowDefinition))
                    .wasApplied()) {
                throw new ConflictException(
                        "Workflow: %s, version: %s already exists!",
                        workflowDef.getName(), workflowDef.getVersion());
            }
            String workflowDefIndex =
                    getWorkflowDefIndexValue(workflowDef.getName(), workflowDef.getVersion());
            session.execute(
                    insertWorkflowDefVersionIndexStatement.bind(
                            workflowDefIndex, workflowDefIndex));
            defIndex.invalidate();
            recordCassandraDaoRequests("createWorkflowDef");
            recordCassandraDaoPayloadSize(
                    "createWorkflowDef", workflowDefinition.length(), "n/a", workflowDef.getName());
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "createWorkflowDef");
            String errorMsg =
                    String.format(
                            "Error creating workflow definition: %s/%d",
                            workflowDef.getName(), workflowDef.getVersion());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @Override
    public void updateWorkflowDef(WorkflowDef workflowDef) {
        try {
            String workflowDefinition = toJson(workflowDef);
            session.execute(
                    updateWorkflowDefStatement.bind(
                            workflowDefinition, workflowDef.getName(), workflowDef.getVersion()));
            String workflowDefIndex =
                    getWorkflowDefIndexValue(workflowDef.getName(), workflowDef.getVersion());
            session.execute(
                    insertWorkflowDefVersionIndexStatement.bind(
                            workflowDefIndex, workflowDefIndex));
            defIndex.invalidate();
            recordCassandraDaoRequests("updateWorkflowDef");
            recordCassandraDaoPayloadSize(
                    "updateWorkflowDef", workflowDefinition.length(), "n/a", workflowDef.getName());
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "updateWorkflowDef");
            String errorMsg =
                    String.format(
                            "Error updating workflow definition: %s/%d",
                            workflowDef.getName(), workflowDef.getVersion());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @Override
    public Optional<WorkflowDef> getLatestWorkflowDef(String name) {
        // One row: the highest version. (It used to read the JSON of every version of the name and
        // sort them, which is very heavy for a name with thousands of versions.)
        try {
            recordCassandraDaoRequests("getLatestWorkflowDef", "n/a", name);
            Row row = session.execute(selectLatestWorkflowDefByNameStatement.bind(name)).one();
            return Optional.ofNullable(row)
                    .map(r -> readValue(r.getString(WORKFLOW_DEFINITION_KEY), WorkflowDef.class));
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getLatestWorkflowDef");
            String errorMsg = String.format("Failed to get the latest workflow def: %s", name);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @Override
    public Optional<WorkflowDef> getWorkflowDef(String name, int version) {
        try {
            recordCassandraDaoRequests("getWorkflowDef");
            ResultSet resultSet = session.execute(selectWorkflowDefStatement.bind(name, version));
            WorkflowDef workflowDef =
                    Optional.ofNullable(resultSet.one())
                            .map(
                                    row ->
                                            readValue(
                                                    row.getString(WORKFLOW_DEFINITION_KEY),
                                                    WorkflowDef.class))
                            .orElse(null);
            return Optional.ofNullable(workflowDef);
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getTaskDef");
            String errorMsg = String.format("Error fetching workflow def: %s/%d", name, version);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @Override
    public void removeWorkflowDef(String name, Integer version) {
        try {
            session.execute(deleteWorkflowDefStatement.bind(name, version));
            session.execute(
                    deleteWorkflowDefIndexStatement.bind(
                            WORKFLOW_DEF_INDEX_KEY, getWorkflowDefIndexValue(name, version)));
            defIndex.invalidate();
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "removeWorkflowDef");
            String errorMsg =
                    String.format("Failed to remove workflow definition: %s/%d", name, version);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @Override
    public List<WorkflowDef> getAllWorkflowDefs() {
        // Every version of every name: the caller asked for all of it. Read through a window of
        // concurrent queries instead of one query after another.
        WorkflowDefIndex.Snapshot index = defIndex.get();
        List<ImmutablePair<String, Integer>> keys = new ArrayList<>(index.entries());
        index.versionsByName()
                .forEach(
                        (name, versions) ->
                                versions.forEach(v -> keys.add(ImmutablePair.of(name, v))));
        return readWorkflowDefs(keys);
    }

    @Override
    public List<WorkflowDef> getAllWorkflowDefsLatestVersions() {
        // The newest version of each name comes from the index; only those definitions are read
        // (concurrently), and the result is kept with the index until it expires or changes.
        WorkflowDefIndex.Snapshot index = defIndex.get();
        List<WorkflowDef> cached = index.latestDefs();
        if (cached != null) {
            return new ArrayList<>(cached);
        }
        List<WorkflowDef> defs = readWorkflowDefs(latestKeys(index, index.names()));
        index.latestDefs(Collections.unmodifiableList(defs));
        return new ArrayList<>(defs);
    }

    /** Names only, from the index: no definition is read. */
    @Override
    public List<String> getWorkflowNames() {
        return defIndex.get().names();
    }

    /** The versions of one name, from the index: no definition is read. */
    @Override
    public List<WorkflowDefSummary> getWorkflowVersions(String name) {
        List<WorkflowDefSummary> out = new ArrayList<>();
        for (int version : defIndex.get().versions(name)) {
            out.add(summary(name, version));
        }
        return out;
    }

    /** Every name with its versions, from the index: no definition is read. */
    @Override
    public Map<String, ? extends Iterable<WorkflowDefSummary>> getWorkflowNamesAndVersions() {
        Map<String, TreeSet<WorkflowDefSummary>> out = new java.util.TreeMap<>();
        defIndex.get()
                .versionsByName()
                .forEach(
                        (name, versions) -> {
                            TreeSet<WorkflowDefSummary> set = new TreeSet<>();
                            versions.forEach(v -> set.add(summary(name, v)));
                            out.put(name, set);
                        });
        return out;
    }

    @Override
    public SearchResult<WorkflowDef> searchWorkflowDefsLatestVersions(int start, int size) {
        return searchWorkflowDefsLatestVersions(start, size, null, null);
    }

    /**
     * One page of the latest versions, sorted by name. Only the page's definitions are read; a
     * filter on the name uses the index, a filter on another field reads the latest definitions
     * (cached).
     */
    @Override
    public SearchResult<WorkflowDef> searchWorkflowDefsLatestVersions(
            int start, int size, String filterField, String filterValue) {
        WorkflowDefIndex.Snapshot index = defIndex.get();
        boolean filtered = filterValue != null && !filterValue.isEmpty();
        if (filtered && !"name".equals(filterField)) {
            String wanted = filterValue.toLowerCase(java.util.Locale.ROOT);
            List<WorkflowDef> matches = new ArrayList<>();
            for (WorkflowDef def : getAllWorkflowDefsLatestVersions()) {
                String value = fieldOf(def, filterField);
                if (value != null && value.toLowerCase(java.util.Locale.ROOT).contains(wanted)) {
                    matches.add(def);
                }
            }
            matches.sort(Comparator.comparing(WorkflowDef::getName));
            int from = Math.min(Math.max(start, 0), matches.size());
            int to = Math.min(from + Math.max(size, 0), matches.size());
            return new SearchResult<>(matches.size(), new ArrayList<>(matches.subList(from, to)));
        }
        List<String> names = index.names();
        if (filtered) {
            String wanted = filterValue.toLowerCase(java.util.Locale.ROOT);
            names.removeIf(n -> !n.toLowerCase(java.util.Locale.ROOT).contains(wanted));
        }
        int from = Math.min(Math.max(start, 0), names.size());
        int to = Math.min(from + Math.max(size, 0), names.size());
        List<WorkflowDef> page = readWorkflowDefs(latestKeys(index, names.subList(from, to)));
        return new SearchResult<>(names.size(), page);
    }

    private static String fieldOf(WorkflowDef def, String field) {
        switch (field == null ? "" : field) {
            case "description":
                return def.getDescription();
            case "ownerEmail":
                return def.getOwnerEmail();
            case "ownerApp":
                return def.getOwnerApp();
            default:
                return def.getName();
        }
    }

    private static WorkflowDefSummary summary(String name, int version) {
        WorkflowDefSummary summary = new WorkflowDefSummary();
        summary.setName(name);
        summary.setVersion(version);
        return summary;
    }

    private static List<ImmutablePair<String, Integer>> latestKeys(
            WorkflowDefIndex.Snapshot index, List<String> names) {
        List<ImmutablePair<String, Integer>> keys = new ArrayList<>(names.size());
        for (String name : names) {
            int latest = index.latestVersion(name);
            if (latest >= 0) {
                keys.add(ImmutablePair.of(name, latest));
            }
        }
        return keys;
    }

    /** The raw {@code name/version} strings of the index partition (a scan, nothing parsed). */
    private List<String> loadIndexEntries() {
        try {
            recordCassandraDaoRequests("getWorkflowDefIndex");
            ResultSet resultSet =
                    session.execute(selectAllWorkflowDefsStatement.bind(WORKFLOW_DEF_INDEX_KEY));
            List<String> entries = new ArrayList<>();
            for (Row row : resultSet) { // pages are fetched as the loop goes
                entries.add(row.getString(WORKFLOW_DEF_NAME_VERSION_KEY));
            }
            return entries;
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getWorkflowDefIndex");
            String errorMsg = "Error reading the workflow definition index";
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    /**
     * Reads the definitions, up to {@link #READ_CONCURRENCY} queries in flight, results in the
     * order of the keys (missing ones skipped).
     */
    private List<WorkflowDef> readWorkflowDefs(List<ImmutablePair<String, Integer>> keys) {
        List<WorkflowDef> out = new ArrayList<>(keys.size());
        ArrayDeque<ResultSetFuture> window = new ArrayDeque<>();
        try {
            for (ImmutablePair<String, Integer> key : keys) {
                if (window.size() >= READ_CONCURRENCY) {
                    addRow(out, window.poll());
                }
                window.add(
                        session.executeAsync(
                                selectWorkflowDefStatement.bind(key.getLeft(), key.getRight())));
            }
            while (!window.isEmpty()) {
                addRow(out, window.poll());
            }
            recordCassandraDaoRequests("getWorkflowDefs");
            return out;
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getWorkflowDefs");
            String errorMsg = "Error reading workflow definitions";
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    private void addRow(List<WorkflowDef> out, ResultSetFuture future) {
        Row row = future.getUninterruptibly().one();
        if (row != null) {
            out.add(readValue(row.getString(WORKFLOW_DEFINITION_KEY), WorkflowDef.class));
        }
    }

    private TaskDef getTaskDefFromDB(String name) {
        try {
            ResultSet resultSet = session.execute(selectTaskDefStatement.bind(name));
            recordCassandraDaoRequests("getTaskDef", name, null);
            return Optional.ofNullable(resultSet.one()).map(this::setDefaults).orElse(null);
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getTaskDef");
            String errorMsg = String.format("Failed to get task def: %s", name);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<TaskDef> getAllTaskDefsFromDB() {
        try {
            ResultSet resultSet = session.execute(selectAllTaskDefsStatement.bind(TASK_DEFS_KEY));
            List<Row> rows = resultSet.all();
            if (rows.size() == 0) {
                LOGGER.info("No task definitions were found.");
                return Collections.EMPTY_LIST;
            }
            return rows.stream().map(this::setDefaults).collect(Collectors.toList());
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getAllTaskDefs");
            String errorMsg = "Failed to get all task defs";
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    private List<WorkflowDef> getAllWorkflowDefVersions(String name) {
        try {
            ResultSet resultSet =
                    session.execute(selectAllWorkflowDefVersionsByNameStatement.bind(name));
            recordCassandraDaoRequests("getAllWorkflowDefVersions", "n/a", name);
            List<Row> rows = resultSet.all();
            if (rows.size() == 0) {
                LOGGER.info("Not workflow definitions were found for : {}", name);
                return null;
            }
            return rows.stream()
                    .map(
                            row ->
                                    readValue(
                                            row.getString(WORKFLOW_DEFINITION_KEY),
                                            WorkflowDef.class))
                    .collect(Collectors.toList());
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getAllWorkflowDefVersions");
            String errorMsg = String.format("Failed to get workflows defs for : %s", name);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    private TaskDef insertOrUpdateTaskDef(TaskDef taskDef) {
        try {
            String taskDefinition = toJson(taskDef);
            session.execute(insertTaskDefStatement.bind(taskDef.getName(), taskDefinition));
            recordCassandraDaoRequests("storeTaskDef");
            recordCassandraDaoPayloadSize(
                    "storeTaskDef", taskDefinition.length(), taskDef.getName(), "n/a");
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "insertOrUpdateTaskDef");
            String errorMsg =
                    String.format("Error creating/updating task definition: %s", taskDef.getName());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
        return taskDef;
    }

    @VisibleForTesting
    String getWorkflowDefIndexValue(String name, int version) {
        return name + INDEX_DELIMITER + version;
    }

    @VisibleForTesting
    ImmutablePair<String, Integer> getWorkflowNameAndVersion(String nameVersionStr) {
        int lastIndexOfDelimiter = nameVersionStr.lastIndexOf(INDEX_DELIMITER);

        if (lastIndexOfDelimiter == -1) {
            throw new IllegalStateException(
                    nameVersionStr
                            + " is not in the 'workflowName"
                            + INDEX_DELIMITER
                            + "version' pattern.");
        }

        String workflowName = nameVersionStr.substring(0, lastIndexOfDelimiter);
        String versionStr = nameVersionStr.substring(lastIndexOfDelimiter + 1);

        try {
            return new ImmutablePair<>(workflowName, Integer.parseInt(versionStr));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    versionStr + " in " + nameVersionStr + " is not a valid number.");
        }
    }

    private TaskDef setDefaults(Row row) {
        TaskDef taskDef = readValue(row.getString(TASK_DEFINITION_KEY), TaskDef.class);
        if (taskDef != null && taskDef.getResponseTimeoutSeconds() == 0) {
            taskDef.setResponseTimeoutSeconds(
                    taskDef.getTimeoutSeconds() == 0 ? ONE_HOUR : taskDef.getTimeoutSeconds() - 1);
        }
        return taskDef;
    }
}
