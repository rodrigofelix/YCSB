/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

//XXXX if we do replication, fix the consistency levels
/**
 * Cassandra 1.0.6 client for YCSB framework
 */
public class CassandraClient10 extends DB {

    static Random random = new Random();
    public static final int Ok = 0;
    public static final int Error = -1;
    public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);
    public int ConnectionRetries;
    public int OperationRetries;
    public String column_family;
    public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
    public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "10";
    public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
    public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "10";
    public static final String USERNAME_PROPERTY = "cassandra.username";
    public static final String PASSWORD_PROPERTY = "cassandra.password";
    public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
    public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
    public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY = "cassandra.scanconsistencylevel";
    public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
    public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.deleteconsistencylevel";
    public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
    public TTransport tr;
    public Cassandra.Client client;
    boolean _debug = false;
    String _table = "";
    private String host = ""; // current host being used to send queries for this client
    private String hosts = ""; // list of available hosts
    DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    public int counter = 1;
    Exception errorexception = null;
    List<Mutation> mutations = new ArrayList<Mutation>();
    Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
    Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
    ColumnParent parent;
    ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {

        if (getProperties().getProperty("hosts") == null) {
            throw new DBException("Required property \"hosts\" missing for CassandraClient");
        }

        // hosts must be reloaded every time a connection is created because some host
        // may be added (or removed) at any time
        host = assignRandomHost();

        column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
        parent = new ColumnParent(column_family);

        ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
                CONNECTION_RETRY_PROPERTY_DEFAULT));
        OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
                OPERATION_RETRY_PROPERTY_DEFAULT));

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        scanConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(SCAN_CONSISTENCY_LEVEL_PROPERTY, SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        deleteConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(DELETE_CONSISTENCY_LEVEL_PROPERTY, DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        Exception connectexception = null;

        for (int retry = 0; retry < ConnectionRetries; retry++) {
            if (_debug) {
                output("Attempt #" + (new Integer(retry)).toString() + " on host " + host);
            }

            connect();

            try {
                tr.open();
                connectexception = null;
                break;
            } catch (Exception e) {
                connectexception = e;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            // gets another host, if the selected one is not connecting
            // in up to 10 attempts
            if (retry % 5 == 4) {
                if (_debug) {
                    output("Rerunning init after trying with " + host);
                }
                host = assignRandomHost();
            }
        }
        if (connectexception != null) {
            output("Unable to connect to " + host + " after " + ConnectionRetries + " tries");
            throw new DBException(connectexception);
        }

        if (username != null && password != null) {
            Map<String, String> cred = new HashMap<String, String>();
            cred.put("username", username);
            cred.put("password", password);
            AuthenticationRequest req = new AuthenticationRequest(cred);
            try {
                client.login(req);
            } catch (Exception e) {
                throw new DBException(e);
            }
        }
    }

    public void output(String value) {
        Date date = new Date();
        System.out.println(new StringBuilder("[").append(dateFormat.format(date)).append("]").append(" ").append(value));
    }

    public String assignRandomHost() {
        return assignRandomHost(true);
    }

    public String assignRandomHost(boolean reload) {
        if (reload) {
            reloadHosts();
        }

        hosts = getProperties().getProperty("hosts");

        String[] allhosts = hosts.split(",");

        if (_debug) {
            output("Hosts list: " + hosts);
        }

        // if no host was set yet
        if ("".equals(host)) {
            host = allhosts[random.nextInt(allhosts.length)];
            if (_debug) {
                output("Setting host for the new client: " + host);
            }
        } else {
            // if a host was already set, gets a different one if there is more than one
            if (allhosts.length > 1) {
                int index;
                String currentHost = host;
                do {
                    index = random.nextInt(allhosts.length);
                    host = allhosts[index];
                } while (host.equals(currentHost));
                if (_debug) {
                    output("Changing host from " + currentHost + " to " + host);
                }
            }
        }

        return host;
    }

    public void connect() {
        // do not set a timeout since some queries (like scans) can reach the timeout
        tr = new TFramedTransport(new TSocket(host, 9160));
        TProtocol proto = new TBinaryProtocol(tr);
        client = new Cassandra.Client(proto);
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void cleanup() throws DBException {
        tr.close();
    }

    public int changeConnection() {
        return changeConnection(false);
    }

    public int changeConnection(boolean force) {
        if (force || counter % 10 == 0) {
            reloadHosts();
            // change host only if the list of hosts has changed since last time
            // or if forced (for instance, if a connection is throwing exceptions)
            // if (force || !hosts.equals(getProperties().getProperty("hosts"))) {
            host = assignRandomHost(false);

            for (int retry = 0; retry < 3; retry++) {
                try {
                    cleanup();
                } catch (DBException ex) {
                    output("Cleanup Exception: " + ex.getMessage());
                }

                connect();

                try {
                    tr.open();
                    _table = "";
                    break;
                } catch (Exception e) {
                    if (retry == 2) {
                        return Error;
                    }
                    host = assignRandomHost();
                }
            }
            // }
        }

        counter++;

        return Ok;
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

        if (changeConnection() == Error) {
            return Error;
        }

        if (!_table.equals(table)) {
            try {
                client.set_keyspace(table);
                _table = table;
            } catch (Exception e) {
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++) {
            try {
                SlicePredicate predicate;
                if (fields == null) {
                    predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));

                } else {
                    ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
                    for (String s : fields) {
                        fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
                    }

                    predicate = new SlicePredicate().setColumn_names(fieldlist);
                }

                List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, readConsistencyLevel);

                if (_debug) {
                    output("Reading key: " + key);
                }

                Column column;
                String name;
                ByteIterator value;
                for (ColumnOrSuperColumn oneresult : results) {

                    column = oneresult.column;
                    name = new String(column.name.array(), column.name.position() + column.name.arrayOffset(), column.name.remaining());
                    value = new ByteArrayByteIterator(column.value.array(), column.value.position() + column.value.arrayOffset(), column.value.remaining());

                    result.put(name, value);
                }

                if (_debug) {
                    output("ConsistencyLevel=" + readConsistencyLevel.toString());
                }

                return Ok;
            } catch (Exception e) {
                output("Read Exception (Thread: " + Thread.currentThread().getId() + "; Host: " + host + "): " + e.toString());
                errorexception = e;
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }

            // change connection if an exception has happened
            if (changeConnection(true) == Error) {
                return Error;
            }
        }

        errorexception.printStackTrace(System.out);
        return Error;

    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set
     * field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {

        if (changeConnection() == Error) {
            return Error;
        }

        if (!_table.equals(table)) {
            try {
                client.set_keyspace(table);
                _table = table;
            } catch (Exception e) {
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++) {

            try {
                SlicePredicate predicate;
                if (fields == null) {
                    predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));

                } else {
                    ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
                    for (String s : fields) {
                        fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
                    }

                    predicate = new SlicePredicate().setColumn_names(fieldlist);
                }

                KeyRange kr = new KeyRange().setStart_key(startkey.getBytes("UTF-8")).setEnd_key(new byte[]{}).setCount(recordcount);

                List<KeySlice> results = client.get_range_slices(parent, predicate, kr, scanConsistencyLevel);

                if (_debug) {
                    output("Scanning startkey: " + startkey);
                }

                HashMap<String, ByteIterator> tuple;
                for (KeySlice oneresult : results) {
                    tuple = new HashMap<String, ByteIterator>();

                    Column column;
                    String name;
                    ByteIterator value;
                    for (ColumnOrSuperColumn onecol : oneresult.columns) {
                        column = onecol.column;
                        name = new String(column.name.array(), column.name.position() + column.name.arrayOffset(), column.name.remaining());
                        value = new ByteArrayByteIterator(column.value.array(), column.value.position() + column.value.arrayOffset(), column.value.remaining());

                        tuple.put(name, value);
                    }

                    result.add(tuple);
                    if (_debug) {
                        output("ConsistencyLevel=" + scanConsistencyLevel.toString());
                    }
                }

                return Ok;
            } catch (Exception e) {
                output("Scan Exception (Thread: " + Thread.currentThread().getId() + "; Host: " + host + "): " + e.toString());
                errorexception = e;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }

            // change connection if an exception has happened
            if (changeConnection(true) == Error) {
                return Error;
            }
        }

        errorexception.printStackTrace(System.out);
        return Error;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {

        if (changeConnection() == Error) {
            return Error;
        }

        if (!_table.equals(table)) {
            try {
                client.set_keyspace(table);
                _table = table;
            } catch (Exception e) {
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++) {
            if (_debug) {
                output("Inserting key: " + key);
            }

            try {
                ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));

                Column col;
                ColumnOrSuperColumn column;
                for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                    col = new Column();
                    col.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
                    col.setValue(ByteBuffer.wrap(entry.getValue().toArray()));
                    col.setTimestamp(System.currentTimeMillis());

                    column = new ColumnOrSuperColumn();
                    column.setColumn(col);

                    mutations.add(new Mutation().setColumn_or_supercolumn(column));
                }

                mutationMap.put(column_family, mutations);
                record.put(wrappedKey, mutationMap);

                client.batch_mutate(record, writeConsistencyLevel);

                mutations.clear();
                mutationMap.clear();
                record.clear();

                if (_debug) {
                    output("ConsistencyLevel=" + writeConsistencyLevel.toString());
                }

                return Ok;
            } catch (Exception e) {
                output("Insert Exception (Thread: " + Thread.currentThread().getId() + "; Host: " + host + "): " + e.toString());
                errorexception = e;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }

            // change connection if an exception has happened
            if (changeConnection(true) == Error) {
                return Error;
            }
        }

        errorexception.printStackTrace(System.out);
        return Error;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key) {

        if (changeConnection() == Error) {
            return Error;
        }

        if (!_table.equals(table)) {
            try {
                client.set_keyspace(table);
                _table = table;
            } catch (Exception e) {
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++) {
            try {
                client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")),
                        new ColumnPath(column_family),
                        System.currentTimeMillis(),
                        deleteConsistencyLevel);

                if (_debug) {
                    output("Delete key: " + key);
                    output("ConsistencyLevel=" + deleteConsistencyLevel.toString());
                }

                return Ok;
            } catch (Exception e) {
                output("Delete Exception (Thread: " + Thread.currentThread().getId() + "; Host: " + host + "): " + e.toString());
                errorexception = e;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }

            // change connection if an exception has happened
            if (changeConnection(true) == Error) {
                return Error;
            }
        }

        errorexception.printStackTrace(System.out);
        return Error;
    }

    public static void main(String[] args) {
        CassandraClient10 cli = new CassandraClient10();

        Properties props = new Properties();

        props.setProperty("hosts", args[0]);
        cli.setProperties(props);

        try {
            cli.init();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
        vals.put("age", new StringByteIterator("57"));
        vals.put("middlename", new StringByteIterator("bradley"));
        vals.put("favoritecolor", new StringByteIterator("blue"));
        int res = cli.insert("usertable", "BrianFrankCooper", vals);
        System.out.println("Result of insert: " + res);

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        HashSet<String> fields = new HashSet<String>();
        fields.add("middlename");
        fields.add("age");
        fields.add("favoritecolor");
        res = cli.read("usertable", "BrianFrankCooper", null, result);
        System.out.println("Result of read: " + res);
        for (String s : result.keySet()) {
            System.out.println("[" + s + "]=[" + result.get(s) + "]");
        }

        res = cli.delete("usertable", "BrianFrankCooper");
        System.out.println("Result of delete: " + res);
    }

    /*
     * public static void main(String[] args) throws TException,
     * InvalidRequestException, UnavailableException,
     * UnsupportedEncodingException, NotFoundException {
     *
     *
     *
     * String key_user_id = "1";
     *
     *
     *
     *
     * client.insert("Keyspace1", key_user_id, new ColumnPath("Standard1", null,
     * "age".getBytes("UTF-8")), "24".getBytes("UTF-8"), timestamp,
     * ConsistencyLevel.ONE);
     *
     *
     * // read single column ColumnPath path = new ColumnPath("Standard1", null,
     * "name".getBytes("UTF-8"));
     *
     * System.out.println(client.get("Keyspace1", key_user_id, path,
     * ConsistencyLevel.ONE));
     *
     *
     * // read entire row SlicePredicate predicate = new SlicePredicate(null, new
     * SliceRange(new byte[0], new byte[0], false, 10));
     *
     * ColumnParent parent = new ColumnParent("Standard1", null);
     *
     * List<ColumnOrSuperColumn> results = client.get_slice("Keyspace1",
     * key_user_id, parent, predicate, ConsistencyLevel.ONE);
     *
     * for (ColumnOrSuperColumn result : results) {
     *
     * Column column = result.column;
     *
     * System.out.println(new String(column.name, "UTF-8") + " -> " + new
     * String(column.value, "UTF-8"));
     *
     * }
     *
     *
     *
     *
     * }
     */
    public void printHeap() {
        int mb = 1024 * 1024;

        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        //Print used memory
        System.err.println("Memory: "
                + (runtime.totalMemory() - runtime.freeMemory()) / mb
                + ", " + runtime.freeMemory() / mb
                + ", " + runtime.totalMemory() / mb
                + ", " + runtime.maxMemory() / mb);
    }
}
