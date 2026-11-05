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
package com.netflix.conductor.tenant;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.dao.MetadataDAO;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DaoTenantMetadataDAOTest {

    private MetadataDAO metadataDAO;
    private DaoTenantMetadataDAO dao;

    @Before
    public void setUp() {
        metadataDAO = mock(MetadataDAO.class);
        dao = new DaoTenantMetadataDAO(metadataDAO);
    }

    private static WorkflowDef wf(String name, int version, String owner) {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setVersion(version);
        def.setOwnerApp(owner);
        def.setCreateTime(1000L + version);
        return def;
    }

    private static TaskDef task(String name, String owner) {
        TaskDef def = new TaskDef(name);
        def.setOwnerApp(owner);
        return def;
    }

    private void givenWorkflows(WorkflowDef... defs) {
        when(metadataDAO.getAllWorkflowDefs()).thenReturn(List.of(defs));
    }

    @Test
    public void listsOnlyTheTenantsDefinitionsSortedByNameAndVersion() {
        givenWorkflows(
                wf("b_flow", 2, "fs"),
                wf("a_flow", 1, "other"),
                wf("b_flow", 1, "fs"),
                wf("a_flow", 2, "fs"),
                wf("no_owner", 1, null));

        List<WorkflowDef> defs = dao.getWorkflowDefsByTenant("fs");

        assertEquals(3, defs.size());
        assertEquals("a_flow", defs.get(0).getName());
        assertEquals(2, defs.get(0).getVersion());
        assertEquals("b_flow", defs.get(1).getName());
        assertEquals(1, defs.get(1).getVersion());
        assertEquals(2, defs.get(2).getVersion());
    }

    @Test
    public void ownerMatchIsExact() {
        givenWorkflows(
                wf("f1", 1, "fs"), wf("f2", 1, "FS"), wf("f3", 1, "fs2"), wf("f4", 1, "fs "));
        assertEquals(List.of("f1"), dao.getWorkflowNamesByTenant("fs"));
        assertTrue(dao.getWorkflowDefsByTenant(null).isEmpty());
    }

    @Test
    public void latestVersionsAreFilteredByOwner() {
        when(metadataDAO.getAllWorkflowDefsLatestVersions())
                .thenReturn(List.of(wf("z", 3, "fs"), wf("a", 2, "other"), wf("m", 1, "fs")));

        List<WorkflowDef> defs = dao.getWorkflowDefsLatestVersionsByTenant("fs");

        assertEquals(List.of("m", "z"), defs.stream().map(WorkflowDef::getName).toList());
    }

    @Test
    public void namesAreDistinctAndSorted() {
        givenWorkflows(wf("zeta", 1, "fs"), wf("alpha", 1, "fs"), wf("zeta", 2, "fs"));
        assertEquals(List.of("alpha", "zeta"), dao.getWorkflowNamesByTenant("fs"));
    }

    @Test
    public void namesAndVersionsGroupSummaries() {
        givenWorkflows(wf("a", 2, "fs"), wf("a", 1, "fs"), wf("b", 1, "fs"), wf("c", 1, "other"));

        Map<String, List<WorkflowDefSummary>> result =
                dao.getWorkflowNamesAndVersionsByTenant("fs");

        assertEquals(List.of("a", "b"), List.copyOf(result.keySet()));
        List<WorkflowDefSummary> a = result.get("a");
        assertEquals(2, a.size());
        assertEquals(1, a.get(0).getVersion());
        assertEquals(2, a.get(1).getVersion());
        assertEquals("a", a.get(0).getName());
        assertEquals(Long.valueOf(1001L), a.get(0).getCreateTime());
    }

    @Test
    public void versionsOfOneWorkflowAreSortedAndScopedToTheTenant() {
        givenWorkflows(wf("a", 3, "fs"), wf("a", 1, "fs"), wf("a", 2, "other"), wf("b", 1, "fs"));

        List<WorkflowDefSummary> versions = dao.getWorkflowVersionsByTenant("fs", "a");

        assertEquals(List.of(1, 3), versions.stream().map(WorkflowDefSummary::getVersion).toList());
    }

    @Test
    public void getByVersionReturnsOwnDefinitionOnly() {
        when(metadataDAO.getWorkflowDef("a", 2)).thenReturn(Optional.of(wf("a", 2, "fs")));
        when(metadataDAO.getWorkflowDef("b", 1)).thenReturn(Optional.of(wf("b", 1, "other")));
        when(metadataDAO.getWorkflowDef("c", 1)).thenReturn(Optional.empty());

        assertTrue(dao.getWorkflowDef("fs", "a", 2).isPresent());
        assertTrue(dao.getWorkflowDef("fs", "b", 1).isEmpty());
        assertTrue(dao.getWorkflowDef("fs", "c", 1).isEmpty());
    }

    @Test
    public void getWithoutVersionUsesTheLatestAndChecksItsOwner() {
        when(metadataDAO.getLatestWorkflowDef("a")).thenReturn(Optional.of(wf("a", 5, "fs")));
        when(metadataDAO.getLatestWorkflowDef("b")).thenReturn(Optional.of(wf("b", 7, "other")));

        assertEquals(5, dao.getWorkflowDef("fs", "a", null).orElseThrow().getVersion());
        // the latest version belongs to another tenant: the tenant gets nothing, not an older one
        assertTrue(dao.getWorkflowDef("fs", "b", null).isEmpty());
    }

    @Test
    public void taskDefsAreFilteredAndSorted() {
        when(metadataDAO.getAllTaskDefs())
                .thenReturn(List.of(task("t2", "fs"), task("x", "other"), task("t1", "fs")));

        assertEquals(
                List.of("t1", "t2"),
                dao.getTaskDefsByTenant("fs").stream().map(TaskDef::getName).toList());
    }

    @Test
    public void taskDefLookupHonoursOwnerMissingAndNotFound() {
        when(metadataDAO.getTaskDef("mine")).thenReturn(task("mine", "fs"));
        when(metadataDAO.getTaskDef("theirs")).thenReturn(task("theirs", "other"));
        when(metadataDAO.getTaskDef("missing")).thenReturn(null);
        when(metadataDAO.getTaskDef("throws")).thenThrow(new NotFoundException("no such task"));

        assertTrue(dao.getTaskDef("fs", "mine").isPresent());
        assertTrue(dao.getTaskDef("fs", "theirs").isEmpty());
        assertTrue(dao.getTaskDef("fs", "missing").isEmpty());
        assertTrue(dao.getTaskDef("fs", "throws").isEmpty());
    }

    @Test
    public void nullListsFromThePersistenceAreEmpty() {
        when(metadataDAO.getAllWorkflowDefs()).thenReturn(null);
        when(metadataDAO.getAllWorkflowDefsLatestVersions()).thenReturn(null);
        when(metadataDAO.getAllTaskDefs()).thenReturn(null);

        assertTrue(dao.getWorkflowDefsByTenant("fs").isEmpty());
        assertTrue(dao.getWorkflowDefsLatestVersionsByTenant("fs").isEmpty());
        assertTrue(dao.getWorkflowNamesByTenant("fs").isEmpty());
        assertTrue(dao.getTaskDefsByTenant("fs").isEmpty());
    }
}
