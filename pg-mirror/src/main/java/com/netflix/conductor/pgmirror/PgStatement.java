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

import java.util.List;

/**
 * One Postgres statement of a mirror message: {@code sql} with numbered placeholders ({@code $1},
 * {@code $2}, ...; each used once, in ascending order) and the matching {@code args}. Arguments are
 * JSON scalars: string, number, boolean or null.
 */
public record PgStatement(String sql, List<Object> args) {}
