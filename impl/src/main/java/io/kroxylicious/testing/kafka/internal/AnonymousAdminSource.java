/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.internal;

import org.apache.kafka.clients.admin.Admin;

public interface AnonymousAdminSource {
    Admin createAnonymousAdmin();
}
