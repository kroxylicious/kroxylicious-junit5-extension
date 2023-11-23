/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.internal;

import org.apache.kafka.clients.admin.Admin;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Indicates that object acts a source of the Kafka Admin client with administrator privileges.
 */
public interface AdminSource {
    /**
     * Creates an admin client with administrator privileges.  It is the caller's responsibility to
     * close the admin client returned.
     *
     * @return admin client
     */
    @NonNull
    Admin createAdmin();
}
