/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.protocol;

import io.trino.spi.QueryId;

import java.time.Instant;

/**
 * SpooledSegmentHandle is used to uniquely identify a spooled segment and to manage its lifecycle.
 */
public interface SpooledSegmentHandle
{
    QueryId queryId();

    String identifier();

    String encoding();

    Instant expirationTime();
}
