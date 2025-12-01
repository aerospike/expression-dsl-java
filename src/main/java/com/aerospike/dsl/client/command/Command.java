/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.dsl.client.command;

public class Command {
    public static final int INFO1_READ				= (1 << 0); // Contains a read operation.
    public static final int INFO1_GET_ALL			= (1 << 1); // Get all bins.
    public static final int INFO1_SHORT_QUERY		= (1 << 2); // Short query.
    public static final int INFO1_BATCH				= (1 << 3); // Batch read or exists.
    public static final int INFO1_XDR				= (1 << 4); // Operation is being performed by XDR.
    public static final int INFO1_NOBINDATA			= (1 << 5); // Do not read the bins.
    public static final int INFO1_READ_MODE_AP_ALL	= (1 << 6); // Involve all replicas in read operation.
    public static final int INFO1_COMPRESS_RESPONSE	= (1 << 7); // Tell server to compress it's response.

    public static final int INFO2_WRITE				= (1 << 0); // Create or update record
    public static final int INFO2_DELETE			= (1 << 1); // Fling a record into the belly of Moloch.
    public static final int INFO2_GENERATION		= (1 << 2); // Update if expected generation == old.
    public static final int INFO2_GENERATION_GT		= (1 << 3); // Update if new generation >= old, good for restore.
    public static final int INFO2_DURABLE_DELETE	= (1 << 4); // Command resulting in record deletion leaves tombstone (Enterprise only).
    public static final int INFO2_CREATE_ONLY		= (1 << 5); // Create only. Fail if record already exists.
    public static final int INFO2_RELAX_AP_LONG_QUERY = (1 << 6); // Treat as long query, but relax read consistency.
    public static final int INFO2_RESPOND_ALL_OPS	= (1 << 7); // Return a result for every operation.

    public static final int INFO3_LAST				= (1 << 0); // This is the last of a multi-part message.
    public static final int INFO3_COMMIT_MASTER		= (1 << 1); // Commit to master only before declaring success.
    // On send: Do not return partition done in scan/query.
    // On receive: Specified partition is done in scan/query.
    public static final int INFO3_PARTITION_DONE	= (1 << 2);
    public static final int INFO3_UPDATE_ONLY		= (1 << 3); // Update only. Merge bins.
    public static final int INFO3_CREATE_OR_REPLACE	= (1 << 4); // Create or completely replace record.
    public static final int INFO3_REPLACE_ONLY		= (1 << 5); // Completely replace existing record only.
    public static final int INFO3_SC_READ_TYPE		= (1 << 6); // See below.
    public static final int INFO3_SC_READ_RELAX		= (1 << 7); // See below.

    // Interpret SC_READ bits in info3.
    //
    // RELAX   TYPE
    //	                strict
    //	                ------
    //   0      0     sequential (default)
    //   0      1     linearize
    //
    //	                relaxed
    //	                -------
    //   1      0     allow replica
    //   1      1     allow unavailable

    public static final int INFO4_TXN_VERIFY_READ		= (1 << 0); // Send transaction version to the server to be verified.
    public static final int INFO4_TXN_ROLL_FORWARD		= (1 << 1); // Roll forward transaction.
    public static final int INFO4_TXN_ROLL_BACK			= (1 << 2); // Roll back transaction.
    public static final int INFO4_TXN_ON_LOCKING_ONLY	= (1 << 4); // Must be able to lock record in transaction.

    public static final byte STATE_READ_AUTH_HEADER = 1;
    public static final byte STATE_READ_HEADER = 2;
    public static final byte STATE_READ_DETAIL = 3;
    public static final byte STATE_COMPLETE = 4;

    public static final byte BATCH_MSG_READ = 0x0;
    public static final byte BATCH_MSG_REPEAT = 0x1;
    public static final byte BATCH_MSG_INFO = 0x2;
    public static final byte BATCH_MSG_GEN = 0x4;
    public static final byte BATCH_MSG_TTL = 0x8;
    public static final byte BATCH_MSG_INFO4 = 0x10;

    public static final int MSG_TOTAL_HEADER_SIZE = 30;
    public static final int FIELD_HEADER_SIZE = 5;
    public static final int OPERATION_HEADER_SIZE = 8;
    public static final int MSG_REMAINING_HEADER_SIZE = 22;
    public static final int COMPRESS_THRESHOLD = 128;
    public static final long CL_MSG_VERSION = 2L;
    public static final long AS_MSG_TYPE = 3L;
    public static final long MSG_TYPE_COMPRESSED = 4L;

    public byte[] dataBuffer;
    public int dataOffset;
    public final int maxRetries;
    public final int serverTimeout;
    public int socketTimeout;
    public int totalTimeout;
    public Long version;

    public Command(int socketTimeout, int totalTimeout, int maxRetries) {
        this.maxRetries = maxRetries;
        this.totalTimeout = totalTimeout;

        if (totalTimeout > 0) {
            this.socketTimeout = (socketTimeout < totalTimeout && socketTimeout > 0)? socketTimeout : totalTimeout;
            this.serverTimeout = this.socketTimeout;
        }
        else {
            this.socketTimeout = socketTimeout;
            this.serverTimeout = 0;
        }
    }
}
