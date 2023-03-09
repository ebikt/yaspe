Yet Another Sql Prometheus Exporter
===================================

Why? Features that cannot be seen in other exporters:

 * Populating multiple metrics from single query
 * Using one column as a timestamp of measurement

Requirements:
=============

At least one of:

 * `python3-aiomysql`
 * `python3-aiopg`

python ≥ 3.7

Configuration
=============

```
# Listen address
listen: localhost:8002

# Timeout on each query
timeout: 30

# Timeout on collect (default: timeout + 3)
# collect_timeout: 35

# Static database configuration
connections:

  mysql_example:
    max_connections: 10
    type: mysql

    # pymysql connection setup fields
    host: mysql.host.db
    port: 3306
    # or unix_socket: /path/to/unix/socket
    db: database_name
    user: user_name
    password: user_password

  postgresql_example:
    max_connections: 10
    type: pgsql

    # psycopg2 uses single parameter (dsn) to specify connection parameters
    dsn: >-
      host=postgresql.host.db
      dbname=database_name
      user=user_namne
      password=user_password

# Dynamic database configuration - command must return KEY='VALUE' or KEY[NUMBER]='VALUE' lines
connections_ucw:
  # Output of command must be shell-like VAR[num]='VALUE' (or VAR='VALUE' if configuration is not in arrays)
  # If arrays are used, then connection configuration is created for each index (suffix "[num]")
  - command: [ /usr/bin/ucw-config, -C/etc/gigamail/common, 'SQLClient{@Database{Name;Host;Port;DBase;User;Password;Slave}}' ]
    # static configuration values
    static_fields:
      type: mysql
      max_connections: 10
    # mapping of vars to configurations
    dynamic_fields:
      # connection name
      connection: CF_SQLClient_Database_Name
      # these have same meaning as in `connections` table
      host:       CF_SQLClient_Database_Host
      port:       CF_SQLClient_Database_Port
      db:         CF_SQLClient_Database_DBase
      user:       CF_SQLClient_Database_User
      password:   CF_SQLClient_Database_Password

# Metrics that are collected
metrics:
  gm_node_flags:
    #prometheus help
    help: Flags currently assigned to node
    #prometheus name
    type: untyped
    # These labels are dimensions, i.e., they define unique "key" for measurement
    dimensions: [ gmn ]
  gm_node_usage:
    help: Percent of used capacity
    type: untyped
    dimensions: [ gmn ]
  gm_node_iowait:
    help: Percent measured of iowait
    type: gauge
    dimensions: [ gmn ]
  gm_node_ver_size:
    help: Total capacity of data storage and version label
    type: gauge
    dimensions: [ gmn ]
  nos_cron_job:
    help: Status of nos cron job
    type: gauge
    dimensions: [ job ]

endpoints:
  list-nodes:
    # List of endpoint queries. Single endpoint may run multiple queries if needed.
    - connection: mysql_example
      # Alternatively run query on multiple servers:
      # connections: [ conn1, conn2 ]
      # conn_label: name_of_connection_label
      # then all produced metrics must have dimension name_of_connection_label
      query: |
        SELECT `node` `gmn`,
               CONCAT( 'gmn', `node`, '.cent') `node_hostname`,
               `groupName` `group_name`,
               `flags` `flag_value`,
               bin(`flags`) `flags`,
               UNIX_TIMESTAMP(`lastUpdate`) * 1000 `update_msec`,
               `nodeLoad`,
               `discUsagePerc`,
               `ioWaitPerc`,
               `discSize`,
               `version`
        FROM `nodes`
      # Columns that are emited as prometheus metrics
      values:
        flag_value:
          metric: gm_node_flags
          # columns that make additional labels, that are not dimensions
          value_labels:
            - node_hostname
            - group_name
            - flags
        discUsagePerc:
          metric: gm_node_usage
          at: update_msec
        ioWaitPerc:
          metric: gm_node_iowait
          at: update_msec
        discSize:
          metric: gm_node_ver_size
          at: update_msec
          value_labels:
            - version
  nos:
    - connection: postgresql_example
      query: |
        SELECT
          "job_type" "job",
          "job_id" "process",
          "updated",
          CASE WHEN running = 0 THEN 'no' ELSE 'yes' END "running",
          CASE WHEN toolong = 0 THEN 'no' ELSE 'yes' END "timeout",
          "running" + "toolong" "state"
        FROM
        ( SELECT
            "job_type",
            "job_id",
            CASE WHEN "finished" IS NULL THEN 1 ELSE 0 END "running",
            CASE WHEN COALESCE("finished", "started") + "max_age" < NOW() THEN 2 ELSE 0 END "toolong",
            ROUND(EXTRACT(EPOCH FROM COALESCE("finished", "started"))*1000) "updated"
          FROM "cron_jobs" ) AS "t"
      values:
        state:
          metric: nos_cron_job
          at: updated
          value_labels:
            - running
            - timeout
            - process
```

aio_exporter and aio_sdnotify
-----------------------------
These should be merged from https://github.com/ebikt/aio_exporter
