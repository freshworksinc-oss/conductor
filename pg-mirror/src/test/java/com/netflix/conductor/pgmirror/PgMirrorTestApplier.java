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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Applies a message the way every consumer must: in one transaction, statements in order; for a
 * gated message the first statement is the guard, and when it affects no row (a stale message) the
 * rest are skipped. {@code $n} placeholders become {@code ?} (each is used once, ascending).
 */
final class PgMirrorTestApplier {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final DataSource target;

    PgMirrorTestApplier(DataSource target) {
        this.target = target;
    }

    /** Goes through JSON, as the message does through Redis. Returns false when it was stale. */
    boolean apply(PgMirrorMessage original) throws Exception {
        PgMirrorMessage m =
                JSON.readValue(JSON.writeValueAsString(original), PgMirrorMessage.class);
        try (Connection c = target.getConnection()) {
            c.setAutoCommit(false);
            try {
                for (int i = 0; i < m.statements().size(); i++) {
                    int rows = run(c, m.statements().get(i));
                    if (i == 0 && m.gated() && rows == 0) {
                        c.rollback();
                        return false;
                    }
                }
                c.commit();
                return true;
            } catch (Exception e) {
                c.rollback();
                throw e;
            }
        }
    }

    private static int run(Connection c, PgStatement s) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(s.sql().replaceAll("\\$\\d+", "?"))) {
            for (int i = 0; i < s.args().size(); i++) {
                Object a = s.args().get(i);
                if (a == null) {
                    ps.setNull(i + 1, Types.VARCHAR);
                } else if (a instanceof Boolean b) {
                    ps.setBoolean(i + 1, b);
                } else if (a instanceof Number n) {
                    ps.setLong(i + 1, n.longValue());
                } else {
                    ps.setString(i + 1, a.toString());
                }
            }
            return ps.executeUpdate();
        }
    }
}
