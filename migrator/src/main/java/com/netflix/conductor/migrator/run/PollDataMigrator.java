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
package com.netflix.conductor.migrator.run;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.migrator.rest.SourceClient;
import com.netflix.conductor.postgres.dao.PostgresPollDataDAO;

/**
 * One-shot copy of the source's poll data (last-poll-time per taskDefName/domain) into the
 * destination {@code poll_data} table via the embedded {@link PostgresPollDataDAO}.
 *
 * <p>Only active when {@code migrator.poll-data.enabled=true} (the DAO bean is gated on the same
 * flag). Poll data is transient operational metadata — workers repopulate it as they poll the dest
 * — so this is a convenience for monitoring continuity, not execution state. Note the DAO's {@code
 * updateLastPollData} stamps {@code lastPollTime=now}, so the migrated timestamp is migration-time,
 * not the source's original poll time (acceptable given the above).
 */
@Component
@ConditionalOnProperty(name = "migrator.poll-data.enabled", havingValue = "true")
public class PollDataMigrator {

    private static final Logger log = LoggerFactory.getLogger(PollDataMigrator.class);

    private final SourceClient source;
    private final PostgresPollDataDAO destPollDataDao;

    public PollDataMigrator(SourceClient source, PostgresPollDataDAO destPollDataDao) {
        this.source = source;
        this.destPollDataDao = destPollDataDao;
    }

    public void migrate() {
        List<PollData> all = source.getAllPollData();
        int migrated = 0;
        for (PollData pd : all) {
            if (pd.getQueueName() == null) {
                continue;
            }
            destPollDataDao.updateLastPollData(pd.getQueueName(), pd.getDomain(), pd.getWorkerId());
            migrated++;
        }
        log.info("Poll data (JDBC): {} entr(y/ies) migrated ({} on source)", migrated, all.size());
    }
}
