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
package io.trino.spi.connector;

import com.google.errorprone.annotations.FormatMethod;

import static java.lang.String.format;

final class Preconditions
{
    private Preconditions() {}

    @FormatMethod
    static void checkArgument(boolean test, String message, Object... args)
    {
        if (!test) {
            throw new IllegalArgumentException(format(message, args));
        }
    }

    @FormatMethod
    static void checkState(boolean test, String message, Object... args)
    {
        if (!test) {
            throw new IllegalStateException(format(message, args));
        }
    }
}
