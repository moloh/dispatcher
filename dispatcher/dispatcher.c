#include "dispatcher.h"

/* TODO: portable printf format for pid_t (now %d) */

/* global buffers */
char        buffer[BUFFER_SIZE_MAX]; /* buffer for various tasks */
dp_buffer  *query = NULL;            /* query buffer */

int main(int argc, char *argv[])
{
    uint32_t dispatched_counter = 0;       /* number of dispatched tasks */
    time_t sense_timestamp = 0;            /* timestamp for sense display */
    time_t terminate_timestamp = 0;        /* timestamp for termination sense display */
    time_t pause_timestamp = 0;            /* timestamp for pause sense display */
    const char *sql;                       /* static query buffer */
    MYSQL *db = NULL;
    int option;

    const char *usage =
        "Usage: "PACKAGE_NAME" [OPTIONS...]\n"
        " -h\t\tPrint this help information\n"
        " -v\t\tShow version information\n"
        " -f <file>\tOverride configuration file location\n"
        " -n <size>\tMaximum number of children\n";

    /* process command line parameters */
    while ((option = getopt(argc, argv, "n:f:hv")) != -1)
        switch (option) {
            case 'n':
                if (sscanf(optarg, "%"SCNu8, &child_limit) < 1) {
                    fprintf(stderr, "Invalid parameter value\n");
                    return EXIT_FAILURE;
                }
                break;
            case 'f':
                cfg_location = optarg;
                break;
            case 'h':
                printf(usage);
                return EXIT_SUCCESS;
            case 'v':
                printf(PACKAGE_NAME", version "PACKAGE_VERSION"\n");
                return EXIT_SUCCESS;
            case '?':
                printf(usage);
                return EXIT_FAILURE;
        }

    /* initialize buffers */
    query = dp_buffer_new(BUFFER_QUERY);

    /* initialize signal and status processing */
    if (!dp_config_init() ||
        !dp_status_init() ||
        !dp_signal_init())
        return EXIT_FAILURE;

    /* specify that we are initialized */
    initialized = true;

    /* initialize logger */
    dp_logger_init(cfg.log.dispatcher);

    /* global MySQL initialize */
    my_init();

    /* initialize MySQL structure */
    if (!dp_mysql_init(&db))
        return EXIT_FAILURE;

    /* connect to MySQL server */
    if (!dp_mysql_connect(db)) {
        mysql_close(db);
        return EXIT_FAILURE;
    }

    printf("Dispatcher started, check syslog for details\n");

    /* main loop */
    while (true) {
        time_t timestamp;
        bool is_job = false;
        dp_task task;
        pid_t pid;

        /* get timestamp for current iteration */
        timestamp = time(NULL);

        /* Check if we should reload configuration */
        if (reload_flag) {
            dp_logger(LOG_WARNING, "Reloading configuration...");
            fprintf(stderr, "Reloading configuration...\n");

            /* reload configuration and log files (possible changes)
             * NOTE: this function automatically merges configuration and
             *       handles errors
             * NOTE: openlog does not allocate identifier!
             */
            if (dp_config_init())
                dp_logger_init(cfg.log.dispatcher);

            reload_flag = false;
        }

        /* check if we should terminate */
        if (terminate_flag) {
            /* if we start countdown then we should print some information
             * to syslog and terminal
             */
            if (terminate_timestamp == 0) {
                dp_logger(LOG_WARNING, "Terminating... Waiting for children");
                fprintf(stderr, "Terminating... Waiting for children\n");
                terminate_timestamp = timestamp + cfg.sense.terminated;
            }

            if (terminate_flag >= FORCE_TERMINATE_COUNT) {
                dp_logger(LOG_WARNING, "Forced instant termination");
                fprintf(stderr, "Forced instant termination\n");

                /* kill all child processes */
                for (size_t i = 0; i < child_limit; ++i)
                    if (!child_status[i].null)
                        kill(child_status[i].pid, SIGKILL);

                break;
            }

            /* Check if we should print sense information during terminate */
            if (cfg.sense.terminated &&
                terminate_timestamp < timestamp) {

                dp_logger(LOG_WARNING, "(%d/%d) Terminating...", child_counter, child_limit);
                fprintf(stderr, "(%d/%d) Terminating...\n", child_counter, child_limit);
                terminate_timestamp = timestamp + cfg.sense.terminated;
            }

            /* Check if all children are done
             * NOTE: we break main loop here
             */
            if (child_counter <= 0) {
                dp_logger(LOG_WARNING, "Terminated");
                fprintf(stderr, "Terminated\n");
                break;
            }

            sleep(cfg.sleep_loop);

            /* update status of workers and get number of workers */
            dp_status_timeout(timestamp - cfg.task.timeout_delay);
            dp_status_update();

            continue;
        }

        /* check if we are paused */
        if (pause_flag) {
            /* update sense timestamp when paused */
            sense_timestamp = timestamp + cfg.sense.loop;

            /* Check if we should print sense information during pause */
            if (cfg.sense.paused &&
                pause_timestamp < timestamp) {

                dp_logger(LOG_NOTICE, "Sleeping (%"PRIi32"/%"PRIi32")",
                          child_counter, child_limit);
                pause_timestamp = timestamp + cfg.sense.paused;
            }

            sleep(cfg.sleep_loop);

            /* update status of workers and get number of workers */
            dp_status_timeout(timestamp - cfg.task.timeout_delay);
            dp_status_update();

            continue;
        } else
            pause_timestamp = timestamp + cfg.sense.paused;

        /* log sense */
        if (cfg.sense.loop &&
            sense_timestamp < timestamp) {

            dp_logger(LOG_NOTICE, "Dispatching (%"PRIu32") (%"PRIi32"/%"PRIi32")",
                      dispatched_counter,
                      child_counter, child_limit);

            sense_timestamp = timestamp + cfg.sense.loop;
            dispatched_counter = 0;
        }

        /* If we are already at maximum fork capacity, we shouldn't
         * grab another task, we should just sleep for a bit.
         * NOTE: this prevents automatic update of task in database, even
         * when there is no space in queue
         */
        if (child_counter >= child_limit) {
            sleep(cfg.sleep_loop);

            /* update status of workers and get number of workers */
            dp_status_timeout(timestamp - cfg.task.timeout_delay);
            dp_status_update();

            continue;
        }

        /* START fake loop for query "exceptions" */
        do {
            MYSQL_RES *result;
            int id = -1;

            /* make double sure that there is space in queue for task,
             * we don't want to make it 'working' and do nothing later
             */
            if (child_counter >= child_limit)
                break;

            /* reset counter */
            if (!dp_mysql_query(db, "SET @id := NULL"))
                break;

            /* setup environment if needed */
            if (cfg.task.environment != NULL)
                snprintf(buffer, BUFFER_SIZE_MAX,
                         "AND type LIKE '%%:%s:%%' ",
                         cfg.task.environment);
            else
                strcpy(buffer, "");

            /* update task to 'working' state and extract its id */
            sql = "UPDATE %s "
                  "SET status = 'working', run_after = '%ld' "
                  "WHERE id = "
                      "(SELECT * FROM (SELECT id FROM %s "
                      "WHERE status IN ('new','working') AND run_after < %ld %s"
                      "ORDER BY priority DESC LIMIT 1) AS innerquery) "
                  "AND @id := id";
            dp_buffer_printf(query, sql,
                             cfg.mysql.table,
                             timestamp + cfg.task.timeout_delay,
                             cfg.mysql.table,
                             timestamp, buffer);
            if (!dp_mysql_query(db, query->str))
                break;

            /* this should always be NULL */
            result = mysql_store_result(db);
            mysql_free_result(result);

            /* extract our reply */
            dp_mysql_query(db, "SELECT @id");
            result = mysql_store_result(db);
            dp_mysql_get_int(&id, result);
            mysql_free_result(result);

            /* check if we get valid reply
             * NOTE: we hit this when there is nothing to do
             */
            if (id < 0)
                break;

            /* extract task */
            sql = "SELECT * FROM %s WHERE id = %d";
            dp_buffer_printf(query, sql,
                             cfg.mysql.table, id);
            if (!dp_mysql_query(db, query->str))
                break;

            result = mysql_store_result(db);
            if (!dp_mysql_get_task(&task, result))
                dp_logger(LOG_ERR, "Failed to get job (%d) details!", id);
            else
                is_job = true;
            mysql_free_result(result);

        /* END fake loop for query "exceptions" */
        } while (false);

        /* START fake loop for dispatching "exceptions" */
        do {
            dp_child *worker;

            /* check if we have free spots
             * NOTE: currently is there is job we get here only if there is
             * free spot in queue, but check of queue size anyway
             */
            if (!is_job || child_counter >= child_limit)
                break;

            /* find first empty entry */
            if ((worker = dp_child_null()) == NULL) {
                dp_logger(LOG_ERR,
                          "Internal job error (%d): No more free workers",
                          task.id);

                /* insert task back to queue */
                sql = "UPDATE %s SET status = 'new' WHERE id = %d";
                dp_buffer_printf(query, sql,
                                 cfg.mysql.table,
                                 worker->task.id);
                dp_mysql_query(db, query->str);

                /* stop processing */
                break;
            }

            /* initialize worker data */
            worker->task = task;
            worker->stamp = timestamp;
            worker->null = false;

            /* update dispatched marker for sense */
            dispatched_counter += 1;

            /* fork worker */
            if ((pid = fork()) == 0)
                /* executed in fork only */
                return dp_fork_exec(worker);

            /* detect fork error */
            /* fork failed */
            if (pid < 0) {
                dp_logger(LOG_ERR, "Fork failed!");

                /* insert task back to queue */
                sql = "UPDATE %s SET status = 'new' WHERE id = %d";
                dp_buffer_printf(query, sql,
                                 cfg.mysql.table,
                                 worker->task.id);
                dp_mysql_query(db, query->str);

            /* fork successful */
            } else {
                /* update worker status */
                worker->pid = pid;

                /* increase task count */
                child_counter += 1;
            }
        /* END fake loop for dispatching "exceptions" */
        } while (false);

        /* wait for next iteration
         * NOTE: sleep terminates when we receive signal
         * NOTE: sleep only when no task are waiting or we have full queue
         */
        if (!is_job || (is_job && child_counter >= child_limit))
            sleep(cfg.sleep_loop);

        /* update status of workers and get number of running workers */
        dp_status_timeout(timestamp - cfg.task.timeout_delay);
        dp_status_update();
    }

    /* close mysql connection */
    mysql_close(db);

    /* free configuration resources */
    dp_buffer_free(query);
    dp_config_free(&cfg);
    dp_status_free();

    return EXIT_SUCCESS;
}

int dp_fork_exec(dp_child *worker)
{
    gearman_client_st *client = NULL;   /* gearman client */
    gearman_return_t error;             /* gearman error */
    void *worker_result = NULL;         /* gearman worker result */
    size_t worker_result_size;          /* gearman worker result size */

    time_t timestamp;                   /* fork execution timestamp */
    const char *sql;                    /* static query buffer */
    MYSQL *db = NULL;

    /* data extracted from worker result */
    char *value, *yaml_elapsed;
    bool status = false;
    double elapsed;

    /* initialize signal processing */
    if (!dp_fork_signal_init())
        return EXIT_FAILURE;

    /* initialize logger */
    dp_logger_init(cfg.log.worker);

    dp_logger(LOG_DEBUG, "Worker forked (%d/%d) job (%d)",
              child_counter + 1, child_limit, worker->task.id);

    /* gearman initialize */
    if (!dp_gearman_init(&client))
        return EXIT_FAILURE;

    /* process job */
    worker_result = gearman_client_do(client,
                                      worker->task.type,
                                      NULL,
                                      worker->task.description,
                                      strlen(worker->task.description),
                                      &worker_result_size,
                                      &error);

    /* entering critical section, blocking terminate signal */
    /* NOTE: we block SIGTRERM till the end */
    dp_signal_block(SIGTERM);

    /* get result timestamp */
    timestamp = time(NULL);

    /* NOTE: we initialize mysql only after gearman finished work.
     *       This prevents timeouts from mysql connection when task execution
     *       takes more time.
     */

    /* initialize mysql for worker */
    my_init();

    /* initialize connection data */
    if (!dp_mysql_init(&db) ||
        !dp_mysql_connect(db))
        return EXIT_FAILURE;

    /* error executing work, retry */
    if (error) {
        dp_logger(LOG_ERR, "Worker job (%d) FAILED (%d)",
                  worker->task.id, error);

        /* update task to describe error */
        sql = "UPDATE %s "
              "SET status = 'failed', result = '%s' "
              "WHERE id = %d";
        dp_buffer_printf(query, sql,
                         cfg.mysql.table,
                         RESULT_ERROR_GEARMAN,
                         worker->task.id);
        if (!dp_mysql_query(db, query->str))
            return EXIT_FAILURE;

        /* escape work details */
        char *type = dp_strescape(worker->task.type);
        char *description = dp_strescape(worker->task.description);

        /* specify run delay */
        time_t run_after = timestamp + cfg.task.failed_delay;

        /* insert task one more time */
        sql = "INSERT INTO %s "
              "(type, description, status, priority, run_after) "
              "VALUES ('%s', '%s', 'new', '%d', '%ld')";
        dp_buffer_printf(query, sql,
                         type, description,
                         worker->task.priority, run_after);
        if (!dp_mysql_query(db, query->str))
            return EXIT_FAILURE;

        return error;
    }

    /* process reply from gearman */
    /* NOTE: reply may contain non-escaped characters */
    status = dp_gearman_get_status(worker_result, worker_result_size);
    value = dp_struescape(worker_result, worker_result_size);

    /* extract time elapsed value from gearman */
    yaml_elapsed = dp_yaml_value_size(worker_result,
                                      worker_result_size,
                                      ":time_elapsed: ");

    /* validate time elapsed value */
    if (yaml_elapsed == NULL ||
        sscanf(yaml_elapsed, "%lf", &elapsed) < 1)
        elapsed = 0.0;

    /* prepare query to database */
    if (status) {

        /* update task entry to indicate that it is done */
        sql = "UPDATE %s "
              "SET status = 'done', result = '', "
                  "result_timestamp = '%ld', time_elapsed = '%.5lf' "
              "WHERE id = %d";
        dp_buffer_printf(query, sql,
                         cfg.mysql.table,
                         timestamp, elapsed,
                         worker->task.id);
        if (!dp_mysql_query(db, query->str))
            return EXIT_FAILURE;
    } else {

        /* update task entry to indicate that it failed */
        /* NOTE: we update task status and insert new one */
        dp_logger(LOG_ERR, "Worker job (%d) FAILED",
                  worker->task.id);

        /* update status of the task */
        sql = "UPDATE %s "
              "SET status = 'failed', result = '%s', "
                  "result_timestamp = '%ld', time_elapsed = '%.5lf' "
              "WHERE id = %d";
        dp_buffer_printf(query, sql,
                         cfg.mysql.table,
                         value, timestamp, elapsed,
                         worker->task.id);
        if (!dp_mysql_query(db, query->str))
            return EXIT_FAILURE;

        /* escape work details */
        char *type = dp_strescape(worker->task.type);
        char *description = dp_strescape(worker->task.description);

        /* specify run delay */
        time_t run_after = timestamp + cfg.task.failed_delay;

        /* insert task to rerun */
        sql = "INSERT INTO %s "
              "(type, description, status, priority, run_after) "
              "VALUES ('%s', '%s', 'new', '%d', '%ld')";
        dp_buffer_printf(query, sql,
                         cfg.mysql.table,
                         type, description,
                         worker->task.priority, run_after);
        if (!dp_mysql_query(db, query->str))
            return EXIT_FAILURE;

        /* free temporary buffers */
        free(description);
        free(type);
    }

    /* close gearman connection */
    gearman_client_free(client);

    /* close mysql connection */
    mysql_close(db);

    dp_logger(LOG_DEBUG, "Worker finished");
    return EXIT_SUCCESS;
}

/* Load dispatcher configuration file, can be called multiple times.
 * When configuration file is invalid then function return false;
 * NOTE: we log in syslog configuration related issues only after
 *       initialization is complete, i.e. on reload
 */
bool dp_config_init()
{
    FILE *fconfig;
    char buffer[BUFFER_SIZE_MAX];
    char value[BUFFER_SIZE_MAX];
    char name[BUFFER_SIZE_MAX];
    dp_config_val field;
    uint32_t line;
    bool is_eof = false, is_error = false;
    dp_config config;

    /* initialize new config */
    memset(&config, 0, sizeof(dp_config));

    /* extract config location */
    const char *config_location = (cfg_location == NULL)?
        DP_CONFIG"/dispatcher.conf":
        cfg_location;

    /* open configuration file */
    fconfig = fopen(config_location, "r");
    if (fconfig == NULL) {
        if (initialized)
            dp_logger(LOG_ERR, "Unable to find configuration file: '%s'", config_location);
        fprintf(stderr, "Unable to find configuration file: '%s'\n", config_location);
        return false;
    }

    /* read each line separately */
    for (line = 1; fgets(buffer, BUFFER_SIZE_MAX, fconfig); ++line) {

        /* omit comments, empty lines */
        if (*buffer == '#' || *buffer == '\n')
            continue;

        /* read configuration directive */
        /* NOTE: we don't care about overruns here
         *       all buffer are with same length
         */
        sscanf(buffer, "%s = %s", name, value);

        /* extract field id */
        field = dp_config_field(name);

        /* try to set configuration variable */
        if (!dp_config_set(&config, field, value, true)) {
            is_error = true;
            break;
        }
    }

    /* finalize read */
    is_eof = feof(fconfig);
    fclose(fconfig);

    /* check for errors */
    if (is_eof && !is_error) {
        /* reload configuration if parse was successful */
        dp_config_free(&cfg);
        cfg = config;
    } else {
        dp_config_free(&config);
        if (initialized)
            dp_logger(LOG_ERR, "Invalid configuration file at line (%"PRIu32")", line);
        fprintf(stderr, "Invalid configuration file at line (%"PRIu32")\n", line);
        return false;
    }

    return true;
}

bool dp_signal_init()
{
    /* setup signals */
    struct sigaction action;
    sigset_t block, no_block;

    sigemptyset(&block);
    sigemptyset(&no_block);

    /* queue multiple SIGCHLD signals, used to prevent zombie children */
    sigaddset(&block, SIGCHLD);
    /* queue multiple SIGUSR12 signals, used to pause and resume dispatching */
    sigaddset(&block, SIGUSR1);
    sigaddset(&block, SIGUSR2);

    /* setup action for SIGCHLD */
    action.sa_handler = dp_sigchld;
    action.sa_mask = block;
    action.sa_flags = 0;
    /* install signal handler */
    sigaction(SIGCHLD, &action, NULL);

    /* setup action for SIGHUP */
    action.sa_handler = dp_sighup;
    action.sa_mask = no_block;
    action.sa_flags = 0;
    /* install signal handler */
    sigaction(SIGHUP, &action, NULL);

    /* setup action for SIGTERM and SIGINT */
    action.sa_handler = dp_sigtermint;
    action.sa_mask = no_block;
    action.sa_flags = 0;
    /* install signal handler */
    sigaction(SIGTERM, &action, NULL);
    sigaction(SIGINT, &action, NULL);

    /* setup action for SIGUSR1 and SIGUSR2 */
    action.sa_handler = dp_sigusr12;
    action.sa_mask = block;
    action.sa_flags = 0;
    /* install signal handler */
    sigaction(SIGUSR1, &action, NULL);
    sigaction(SIGUSR2, &action, NULL);

    return true;
}

bool dp_fork_signal_init()
{
    /* setup signals */
    struct sigaction action;
    sigset_t empty;

    sigemptyset(&empty);

    /* restore default handler */
    action.sa_handler = SIG_DFL;
    action.sa_mask = empty;
    action.sa_flags = 0;
    /* install signal handler */
    sigaction(SIGTERM, &action, NULL);

    return true;
}

bool dp_signal_block(int signum)
{
    sigset_t mask;

    /* specify sigset_t */
    sigemptyset(&mask);
    sigaddset(&mask, signum);

    /* block signal */
    sigprocmask(SIG_BLOCK, &mask, NULL);

    return true;
}

bool dp_signal_unblock(int signum)
{
    sigset_t mask;

    /* specify sigset_t */
    sigemptyset(&mask);
    sigaddset(&mask, signum);

    /* unblock signal */
    sigprocmask(SIG_UNBLOCK , &mask, NULL);

    return true;
}

dp_config_val dp_config_field(const char *name)
{
    if (name == NULL)
        return DP_CONFIG_UNKNOWN;

    if (!strcmp(name, "mysql_host"))
        return DP_CONFIG_MYSQL_HOST;
    if (!strcmp(name, "mysql_db"))
        return DP_CONFIG_MYSQL_DB;
    if (!strcmp(name, "mysql_user"))
        return DP_CONFIG_MYSQL_USER;
    if (!strcmp(name, "mysql_passwd"))
        return DP_CONFIG_MYSQL_PASSWD;
    if (!strcmp(name, "mysql_table"))
        return DP_CONFIG_MYSQL_TABLE;
    if (!strcmp(name, "mysql_port"))
        return DP_CONFIG_MYSQL_PORT;
    if (!strcmp(name, "gearman_host"))
        return DP_CONFIG_GEARMAN_HOST;
    if (!strcmp(name, "gearman_port"))
        return DP_CONFIG_GEARMAN_PORT;
    if (!strcmp(name, "task_failed_delay"))
        return DP_CONFIG_TASK_FAILED_DELAY;
    if (!strcmp(name, "task_timeout_delay"))
        return DP_CONFIG_TASK_TIMEOUT_DELAY;
    if (!strcmp(name, "task_environment"))
        return DP_CONFIG_TASK_ENVIRONMENT;
    if (!strcmp(name, "log_dispatcher"))
        return DP_CONFIG_LOG_DISPATCHER;
    if (!strcmp(name, "log_worker"))
        return DP_CONFIG_LOG_WORKER;
    if (!strcmp(name, "log_level"))
        return DP_CONFIG_LOG_LEVEL;
    if (!strcmp(name, "log_facility"))
        return DP_CONFIG_LOG_FACILITY;
    if (!strcmp(name, "sense_loop"))
        return DP_CONFIG_SENSE_LOOP;
    if (!strcmp(name, "sense_terminated"))
        return DP_CONFIG_SENSE_TERMINATED;
    if (!strcmp(name, "sense_paused"))
        return DP_CONFIG_SENSE_PAUSED;
    if (!strcmp(name, "sleep_loop"))
        return DP_CONFIG_SLEEP_LOOP;

    return DP_CONFIG_UNKNOWN;
}

bool dp_config_set(dp_config *config, dp_config_val field, char *value, bool if_dup)
{
    dp_enum *enumeration;

    switch (field) {
    case DP_CONFIG_UNKNOWN:
        return false;
    case DP_CONFIG_MYSQL_HOST:
        if (config->mysql.host) free(config->mysql.host);
        if (!if_dup) config->mysql.host = value;
        else config->mysql.host = dp_strdup(value);
        break;
    case DP_CONFIG_MYSQL_DB:
        if (config->mysql.db) free(config->mysql.db);
        if (!if_dup) config->mysql.db = value;
        else config->mysql.db = dp_strdup(value);
        break;
    case DP_CONFIG_MYSQL_USER:
        if (config->mysql.user) free(config->mysql.user);
        if (!if_dup) config->mysql.user = value;
        else config->mysql.user = dp_strdup(value);
        break;
    case DP_CONFIG_MYSQL_PASSWD:
        if (config->mysql.passwd) free(config->mysql.passwd);
        if (!if_dup) config->mysql.passwd = value;
        else config->mysql.passwd = dp_strdup(value);
        break;
    case DP_CONFIG_MYSQL_TABLE:
        if (config->mysql.table) free(config->mysql.table);
        if (!if_dup) config->mysql.table = value;
        else config->mysql.table = dp_strdup(value);
        break;
    case DP_CONFIG_MYSQL_PORT:
        if (sscanf(value, "%d", &config->mysql.port) != 1)
            return false;
        break;
    case DP_CONFIG_GEARMAN_HOST:
        if (config->gearman.host) free(config->gearman.host);
        if (!if_dup) config->gearman.host = value;
        else config->gearman.host = dp_strdup(value);
        break;
    case DP_CONFIG_GEARMAN_PORT:
        if (sscanf(value, "%d", &config->gearman.port) != 1)
            return false;
        break;
    case DP_CONFIG_TASK_FAILED_DELAY:
        if (sscanf(value, "%" SCNu16, &config->task.failed_delay) != 1)
            return false;
        break;
    case DP_CONFIG_TASK_TIMEOUT_DELAY:
        if (sscanf(value, "%" SCNu16, &config->task.timeout_delay) != 1)
            return false;
        break;
    case DP_CONFIG_TASK_ENVIRONMENT:
        if (config->task.environment) free(config->task.environment);

        /* handle processing of the environment */
        if (!strcmp(value, "any")) {
            /* free value if we should take ownership */
            if (!if_dup) free(value);
            config->task.environment = NULL;
        } else {
            if (!if_dup) config->task.environment = value;
            else config->task.environment = dp_strdup(value);
        }

        break;
    case DP_CONFIG_LOG_DISPATCHER:
        if (config->log.dispatcher) free(config->log.dispatcher);
        if (!if_dup) config->log.dispatcher = value;
        else config->log.dispatcher = dp_strdup(value);
        break;
    case DP_CONFIG_LOG_WORKER:
        if (config->log.worker) free(config->log.worker);
        if (!if_dup) config->log.worker = value;
        else config->log.worker = dp_strdup(value);
        break;
    case DP_CONFIG_LOG_LEVEL:
        if ((enumeration = dp_enum_name(dp_log_level, value)) == NULL)
            return false;
        config->log.level = enumeration->value;
        break;
    case DP_CONFIG_LOG_FACILITY:
        if ((enumeration = dp_enum_name(dp_log_facility, value)) == NULL)
            return false;
        config->log.facility = enumeration->value;
        break;
    case DP_CONFIG_SENSE_LOOP:
        if (sscanf(value, "%" SCNu16, &config->sense.loop) != 1)
            return false;
        break;
    case DP_CONFIG_SENSE_TERMINATED:
        if (sscanf(value, "%" SCNu16, &config->sense.terminated) != 1)
            return false;
        break;
    case DP_CONFIG_SENSE_PAUSED:
        if (sscanf(value, "%" SCNu16, &config->sense.paused) != 1)
            return false;
        break;
    case DP_CONFIG_SLEEP_LOOP:
        if (sscanf(value, "%" SCNu16, &config->sleep_loop) != 1)
            return false;
        break;
    }

    return true;
}

void dp_config_free(dp_config *config)
{
    if (config != NULL) {
        free(config->mysql.host);
        free(config->mysql.db);
        free(config->mysql.user);
        free(config->mysql.passwd);
        free(config->mysql.table);
        free(config->gearman.host);
        free(config->task.environment);
        free(config->log.dispatcher);
        free(config->log.worker);
    }
}

dp_buffer *dp_buffer_new(size_t pool)
{
    dp_buffer *buf;

    /* allocate buffer */
    buf = malloc(sizeof(dp_buffer));
    if (buf == NULL)
        return NULL;

    /* allocate buffer pool */
    if (dp_buffer_init(buf, pool) == NULL) {
        free(buf);
        return NULL;
    }

    return buf;
}

dp_buffer *dp_buffer_init(dp_buffer *buf, size_t pool)
{
    /* allocate pool */
    buf->str = malloc(pool);
    if (buf->str == NULL)
        return NULL;

    /* initialize buffer */
    buf->str[0] = '\0';
    buf->pool = pool;

    return buf;
}

void dp_buffer_free(dp_buffer *buf)
{
    if (buf != NULL) {
        free(buf->str);
        free(buf);
    }
}

dp_buffer *dp_buffer_printf(dp_buffer *buf, const char *format, ...)
{
    va_list arg;
    size_t len;

    /* insert format string */
    va_start(arg, format);
    len = vsnprintf(buf->str, buf->pool, format, arg);
    va_end(arg);

    /* check if everything was inserted */
    if (len >= buf->pool) {

        /* allocate bigger buffer */
        char *str = malloc(len + 1);
        if (str == NULL) {
            /* failback buffer */
            buf->str[0] = '\0';

            /* log our problem */
            dp_logger(LOG_ERR, "Memory exhaustion!");

            return NULL;
        }

        /* retry insert format string */
        va_start(arg, format);
        vsnprintf(str, len + 1, format, arg);
        va_end(arg);

        /* adjust buffer */
        free(buf->str);
        buf->str = str;
        buf->pool = len + 1;
    }

    return buf;
}

/* basic, logged initialization of gearman */
bool dp_gearman_init(gearman_client_st **client)
{
    if ((*client = gearman_client_create(NULL)) == NULL) {
        dp_logger(LOG_ERR, "Gearman initialization failed");
        return false;
    }

    if (gearman_client_add_server(*client, cfg.gearman.host,
                                           cfg.gearman.port)) {
        dp_logger(LOG_ERR,
                  "Gearman connection error (%d): %s",
                  gearman_client_errno(*client),
                  gearman_client_error(*client));
        return false;
    }

    dp_logger(LOG_INFO, "Gearman connection established");
    return true;
}

/* TODO: maybe extend it (handle first word (hash, list) correctly, discard end lines and white spaces */
/* simple "parser" for YAML reply from gearman */
bool dp_gearman_get_reply(dp_reply *reply, const char *result, size_t size)
{
    const char *str = result, *end;
    const char *last = result + size;
    dp_reply_val field;
    char *name = NULL;

    /* initialize */
    memset(reply, 0, sizeof(dp_reply));

    /* import values */
    for (end = str; end < last; ++end) {
        if (*end == ':') {
            /* check if we have previous data to save */
            if (name != NULL) {
                char *value = NULL;

                /* check if value ends with end line */
                if (end > str && *(end - 1) == '\n')
                    value = dp_strudup(str, end - str - 1);
                else
                    value = dp_strudup(str, end - str);

                /* import value, free if fail */
                field = dp_gearman_reply_field(name);
                if (!dp_gearman_reply_set(reply, field, value))
                    free(value);

                /* clear name */
                free(name);
                name = NULL;
            }

            /* update start position */
            str = end;

            /* find terminating ': ' */
            if ((end = dp_strustr(++end, last - end, ": ")) == NULL) {
                dp_logger(LOG_ERR, "Invalid Gearman reply string");
                return false;
            }

            /* copy name part ":[^:]*:" */
            name = dp_strudup(str, ++end - str);

            /* update position */
            str = ++end;
        }

        /* go to the end of line */
        for (; end < last; ++end)
            if (*end == '\n')
                break;
    }

    if (name != NULL) {
        char *value = dp_strudup(str, end - str);
        field = dp_gearman_reply_field(name);
        if (!dp_gearman_reply_set(reply, field, value))
            free(value);
        free(name);
    }

    return false;
}

bool dp_gearman_get_status(const char *result, size_t size)
{
    /* find status string */
    const char *pos = dp_strustr(result, size, ":status: ");
    if (pos == NULL)
        return false;

    /* check if we have enough remaining space */
    const size_t length = pos + 9 + 3 - result;
    if (length > size || memcmp(pos + 9, ":ok", 3))
        return false;

    return true;
}

/* set single "hash" value from gearman parser */
bool dp_gearman_reply_set(dp_reply *reply, dp_reply_val field, char *value)
{
    switch (field) {
    case DP_REPLY_UNKNOWN:
        return false;
    case DP_REPLY_BACKTRACE:
        if (reply->backtrace) free(reply->backtrace);
        reply->backtrace = value;
        break;
    case DP_REPLY_ERROR:
        if (reply->error) free(reply->error);
        reply->error = value;
        break;
    case DP_REPLY_STATUS:
        if (reply->status) free(reply->status);
        reply->status = value;
        break;
    case DP_REPLY_RESULT:
        if (reply->result) free(reply->result);
        reply->result = value;
        break;
    case DP_REPLY_MESSAGE:
        if (reply->message) free(reply->message);
        reply->message = value;
        break;
    }

    return true;
}

bool dp_gearman_reply_escape(dp_reply *reply, dp_reply_val field)
{
    char **value = NULL, *escape;

    switch (field) {
    case DP_REPLY_UNKNOWN:
        return false;
    case DP_REPLY_BACKTRACE:
        value = &reply->backtrace;
        break;
    case DP_REPLY_ERROR:
        value = &reply->error;
        break;
    case DP_REPLY_STATUS:
        value = &reply->status;
        break;
    case DP_REPLY_RESULT:
        value = &reply->result;
        break;
    case DP_REPLY_MESSAGE:
        value = &reply->message;
        break;
    }

    /* check if there is data to escape */
    if (*value == NULL)
        return true;

    /* escape string */
    escape = dp_strescape(*value);
    if (escape == NULL)
        return false;

    /* set new escaped version of string */
    free(*value);
    *value = escape;

    return true;
}

dp_reply_val dp_gearman_reply_field(const char *name)
{
    if (name == NULL)
        return DP_REPLY_UNKNOWN;

    if (!strcmp(name, ":backtrace:"))
        return DP_REPLY_BACKTRACE;
    else if (!strcmp(name, ":error:"))
        return DP_REPLY_ERROR;
    else if (!strcmp(name, ":status:"))
        return DP_REPLY_STATUS;
    else if (!strcmp(name, ":result:"))
        return DP_REPLY_RESULT;
    else if (!strcmp(name, ":message:"))
        return DP_REPLY_MESSAGE;
    return DP_REPLY_UNKNOWN;
}

const char *dp_gearman_reply_value(dp_reply *reply, dp_reply_val field)
{
    switch (field) {
    case DP_REPLY_BACKTRACE:
        return reply->backtrace;
    case DP_REPLY_ERROR:
        return reply->error;
    case DP_REPLY_STATUS:
        return reply->status;
    case DP_REPLY_RESULT:
        return reply->result;
    case DP_REPLY_MESSAGE:
        return reply->message;
    default:
        return NULL;
    }
}

void dp_gearman_reply_free(dp_reply *reply)
{
    if (reply != NULL) {
        free(reply->backtrace);
        free(reply->error);
        free(reply->message);
        free(reply->result);
        free(reply->status);
    }
}

/* basic, logged initialization of MySQL */
bool dp_mysql_init(MYSQL **db)
{
    if ((*db = mysql_init(*db)) == NULL) {
        dp_logger(LOG_ERR, "MySQL initialization error");
        return false;
    }

    return true;
}

/* reusable logged MySQL connect function */
bool dp_mysql_connect(MYSQL *db)
{
    /* check for valid MySQL structure */
    if (!db) return false;

    /* try open connection */
    if (!mysql_real_connect(db, cfg.mysql.host,
                                cfg.mysql.user,
                                cfg.mysql.passwd,
                                cfg.mysql.db,
                                cfg.mysql.port, NULL, /* port, socket */
                                0)) {                 /* client flags */
        dp_logger(LOG_ERR, "MySQL connection error: %s", mysql_error(db));
        return false;
    }

    dp_logger(LOG_INFO, "MySQL connection established");
    return true;
}

/* logged MySQL query wrapper with basic recovering */
bool dp_mysql_query(MYSQL *db, const char *query)
{
    MYSQL_RES *result;

    /* check if we have query at all */
    if (query == NULL) {
        dp_logger(LOG_ERR, "MySQL query is missing!");
        return false;
    }

    /* execute query */
    if (mysql_query(db, query)) {
        switch (mysql_errno(db)) {
            case CR_COMMANDS_OUT_OF_SYNC:  /* try to clear error */
                dp_logger(LOG_ERR, "MySQL query error (%d), clearing: %s",
                          mysql_errno(db),
                          mysql_error(db));

                result = mysql_store_result(db);
                mysql_free_result(result);
                return false;
            case CR_SERVER_GONE_ERROR:     /* try to reconnect and rerun query */
            case CR_SERVER_LOST:
                dp_logger(LOG_ERR, "MySQL query error (%d), recovering: %s",
                          mysql_errno(db),
                          mysql_error(db));

                /* reconnect to server */
                if (!dp_mysql_connect(db))
                    return false;

                /* execute query, recover */
                if (mysql_query(db, query)) {
                    dp_logger(LOG_ERR, "MySQL query recovery error (%d): %s",
                              mysql_errno(db),
                              mysql_error(db));

                    /* clear state anyway */
                    result = mysql_store_result(db);
                    mysql_free_result(result);
                    return false;
                }

                return true;
            default:
                return false;
        }
    }

    return true;
}

/* TODO: error checking for mysql_fetch_row */
/* convert single row into typed task */
bool dp_mysql_get_task(dp_task *task, MYSQL_RES *result)
{
    MYSQL_FIELD *field;
    MYSQL_ROW row;
    size_t size;

    /* init task structure */
    memset(task, 0, sizeof(dp_task));

    if (result == NULL)
        return false;

    /* fetch single result */
    row = mysql_fetch_row(result);
    size = mysql_num_fields(result);

    /* process single row */
    for (size_t i = 0; i < size; ++i) {

        /* get field */
        mysql_field_seek(result, i);
        field = mysql_fetch_field(result);

        /* check for null value */
        if (row[i] == NULL)
            continue;

        /* convert row into typed task */
        if (!strcmp(field->name, "id"))
            sscanf(row[i], "%d", &task->id);
        else if (!strcmp(field->name, "type"))
            task->type = row[i];
        else if (!strcmp(field->name, "description")) {
            task->description = row[i];
        } else if (!strcmp(field->name, "status"))
            task->status = row[i];
        else if (!strcmp(field->name, "priority"))
            sscanf(row[i], "%d", &task->priority);
        else if (!strcmp(field->name, "run_after"))
            sscanf(row[i], "%ld", &task->run_after);
    }

    /* postprocess fields, allocate strings */
    if (task->type)
        task->type = dp_strdup(task->type);
    if (task->description)
        task->description = dp_strdup(task->description);
    if (task->status)
        task->status = dp_strdup(task->status);

    return true;
}

bool dp_mysql_get_int(int *value, MYSQL_RES *result)
{
    MYSQL_ROW row;

    if (result == NULL)
        return false;

    /* fetch single result */
    row = mysql_fetch_row(result);

    /* validate row */
    if (mysql_num_rows(result) == 1 && mysql_num_fields(result) == 1)
        if (row[0] != NULL)
            sscanf(row[0], "%d", value);

    return true;
}

void dp_mysql_task_free(dp_task *task)
{
    if (task != NULL) {
        free(task->type);
        free(task->description);
        free(task->status);
    }
}

void dp_mysql_task_clear(dp_task *task)
{
    task->id = 0;
    task->priority = 0;
    task->type = NULL;
    task->status = NULL;
    task->description = NULL;
}

char *dp_yaml_value(const char *yaml, const char *field)
{
    char *pos, *end;

    pos = strstr(yaml, field);
    if (pos == NULL)
        return NULL;

    /* move to end of the string */
    pos += strlen(field);

    end = strchr(pos, '\n');
    if (end == NULL)
        return NULL;

    return dp_strudup(pos, end - pos);
}

char *dp_yaml_value_size(const char *yaml, size_t size, const char *field)
{
    char *pos, *end;

    pos = dp_strustr(yaml, size, field);
    if (pos == NULL)
        return NULL;

    /* move to end of the string */
    pos += strlen(field);

    end = dp_struchr(pos, size - (pos - yaml), '\n');
    if (end == NULL)
        return NULL;

    return dp_strudup(pos, end - pos);
}

char *dp_strdup(const char *str)
{
    char *dup = NULL;
    size_t length;

    if (str != NULL) {
        /* get length */
        length = strlen(str);

        /* allocate memory */
        if ((dup = malloc(length + 1)) == NULL)
            return NULL;

        /* copy string */
        memcpy(dup, str, length + 1);
    }

    return dup;
}

char *dp_strudup(const char *str, size_t length)
{
    char *dup;

    if ((dup = malloc((length + 1))) == NULL)
        return NULL;

    /* copy string, only size! */
    memcpy(dup, str, length);
    dup[length] = '\0';

    return dup;
}

char *dp_struchr(const char *str, size_t length, char character)
{
    for (size_t pos = 0; pos < length; ++pos)
        if (str[pos] == character)
            return (char *)str + pos;
    return NULL;
}

char *dp_strustr(const char *str, size_t length, const char *locate)
{
    register size_t pos = 0, x;

    /* check if we have string to locate */
    if (locate == NULL || !*locate)
        return (char *)str;

    for (; pos < length; ++pos) {

        /* check first character */
        if (*locate == str[pos]) {

            /* validate rest of string */
            for (x = 1; pos + x < length; ++x)
                if (locate[x] != str[pos + x])
                    break;

            /* check if we found locate */
            if (!locate[x])
                return (char *)str + pos;
        }
    }

    /* not found */
    return NULL;
}

char *dp_strcat(const char *str, ...)
{
    const char *item;
    va_list args;
    size_t size;
    char *cat;

    if (str == NULL)
        return NULL;

    /* initialize size with first string */
    size = strlen(str);

    /* compute total size of string */
    va_start(args, str);
    while ((item = va_arg(args, const char *)) != NULL)
        size += strlen(item);
    va_end(args);

    /* allocate cat storage */
    cat = malloc((size + 1) * sizeof(char *));
    if (cat == NULL)
        return NULL;

    /* initialize cat with first string */
    for (item = str; *item != '\0'; ++cat, ++item)
        *cat = *item;

    /* copy strings */
    va_start(args, str);
    while ((item = va_arg(args, const char *)) != NULL)
        for (; *item != '\0'; ++cat, ++item)
            *cat = *item;
    va_end(args);

    /* finalize string */
    *cat = '\0';

    /* return string */
    return cat - size;
}

char *dp_strescape(const char *str)
{
    const char *pos;
    char *escape, *esc;
    size_t size = 0;

    /* check if there is data to escape */
    if (str == NULL)
        return NULL;

    /* check number of characters to escape */
    for (pos = str; *pos; ++pos)
        switch (*pos) {
        case '\\':
        case '\'':
            size += 1;
        }

    /* string length (remember about null terminator) */
    size += pos - str + 1;

    /* allocate required escape buffer */
    if ((escape = malloc(size)) == NULL)
        return NULL;

    /* escape string */
    for (pos = str, esc = escape; *pos; ++pos, ++esc)
        switch (*pos) {
        case '\\':
        case '\'':
            *esc++ = '\\';
        default:
            *esc = *pos;
        }

    /* finalize string */
    *esc = '\0';

    return escape;
}

char *dp_struescape(const char *str, size_t length)
{
    char *escape, *esc;
    size_t i, size = length + 1;

    /* check number of characters to escape */
    for (i = 0; i < length; ++i)
        switch (str[i]) {
        case '\\':
        case '\'':
            size += 1;
        }

    /* allocate required escape buffer */
    if ((escape = malloc(size)) == NULL)
        return NULL;

    /* escape string */
    for (i = 0, esc = escape; i < length; ++i, ++esc)
        switch (str[i]) {
        case '\\':
        case '\'':
            *esc++ = '\\';
        default:
            *esc = str[i];
        }

    /* finalize string */
    *esc = '\0';

    return escape;
}

void dp_logger_init(const char *ident)
{
    closelog();
    openlog(ident, LOG_PID, cfg.log.facility);
}

void dp_logger(int priority, const char *message, ...)
{
    va_list args;

    /* check if we should log at all */
    if (priority > cfg.log.level)
        return;

    va_start(args, message);
    vsyslog(priority, message, args);
    va_end(args);
}

dp_enum *dp_enum_name(dp_enum *self, const char *name)
{
    for (; self->name; self += 1)
        if (!strcmp(self->name, name))
            return self;
    return NULL;
}

dp_enum *dp_enum_value(dp_enum *self, int value)
{
    for (; self->name; self += 1)
        if (self->value == value)
            return self;
    return NULL;
}

void dp_sigchld(int signal)
{
    /* remove unused parameter warning */
    (void)signal;

    /* update flag to note pending processing */
    child_flag = 1;
}

void dp_sighup(int signal)
{
    /* remove unused parameter warning */
    (void)signal;

    /* dispatcher should reload configuration */
    reload_flag = 1;
}

void dp_sigtermint(int signal)
{
    /* remove unused parameter warning */
    (void)signal;

    /* dispatcher should terminate */
    terminate_flag += 1;
}

void dp_sigusr12(int signal)
{
    /* remove unused parameter warning */
    (void)signal;

    /* Toggle flag to pause or resume dispatching */
    if (pause_flag)
        pause_flag = 0;
    else
        pause_flag = 1;
}

bool dp_status_init()
{
    /* initialize status array */
    child_status = malloc(child_limit * sizeof(dp_child));
    if (child_status == NULL)
        return false;

    for (size_t i = 0; i < child_limit; ++i) {
        /* clear task */
        dp_mysql_task_clear(&child_status[i].task);

        /* clear other info */
        child_status[i].pid = 0;
        child_status[i].stamp = 0;
        child_status[i].null = true;
    }

    return true;
}

void dp_status_free()
{
    if (child_status != NULL) {
        /* free all data of non null children */
        for (size_t i = 0; i < child_limit; ++i)
            if (!child_status[i].null)
                dp_mysql_task_free(&child_status[i].task);
        /* free status table array */
        free(child_status);
    }
}

void dp_status_update()
{
    int status;
    pid_t pid;

    /* check if we have pending events */
    if (!child_flag)
        return;

    while ((pid = waitpid(0, &status, WNOHANG)) > 0) {
        dp_child *worker = NULL;
        bool is_end = false;

        /* NOTE: we update flag only after successful retrieval of reaped
         * process
         */

        /* update flag */
        child_flag = false;

        /* extract information about specific child */
        if ((worker = dp_child_pid(pid)) == NULL) {
            dp_logger(LOG_ERR, "Unknown child pid (%d)!",
                      pid);
            continue;
        }

        /* check if normal exit */
        if (WIFEXITED(status)) {
            child_counter -= 1;
            is_end = true;

            if (WEXITSTATUS(status) != EXIT_SUCCESS)
                dp_logger(LOG_ERR,
                          "Child worker (%d) job (%d) invalid exit (%d)",
                          worker->pid, worker->task.id, WEXITSTATUS(status));

        /* check type of signal */
        } else if (WIFSIGNALED(status)) {
            if (WTERMSIG(status) || WCOREDUMP(status)) {
                /* NOTE: when child terminates then we do not update tasks
                 *       table, in case of timeout work will be automatically
                 *       done by dispatcher. When child will die for some
                 *       reason then specific work will be processed after
                 *       timeout.
                 */

                /* log termination, check whether we terminated child because
                 * of timeout
                 */
                if (worker->stamp == 0) {
                    char *class = dp_yaml_value(worker->task.description, ":class: ");
                    char *method = dp_yaml_value(worker->task.description, ":method_name: ");

                    dp_logger(LOG_ERR,
                              "Child worker (%d) job (%d) timeout (%s -> %s)",
                              worker->pid, worker->task.id,
                              class, method);

                    free(class);
                    free(method);
                } else
                    dp_logger(LOG_ERR,
                              "Child worker (%d) job (%d) terminated",
                              worker->pid, worker->task.id);

                child_counter -= 1;
                is_end = true;
            } else
                dp_logger(LOG_ERR,
                          "Child worker (%d) signalled",
                          worker->pid);
        } else
            dp_logger(LOG_ERR,
                      "Invalid child (%d) worker state",
                      worker->pid);

        if (is_end) {
            /* clear task data */
            dp_mysql_task_free(&worker->task);
            dp_mysql_task_clear(&worker->task);

            /* clear worker data */
            worker->pid = 0;
            worker->stamp = 0;
            worker->null = true;
        }
    }

    /* validate size of queue */
    if (child_counter < 0)
        dp_logger(LOG_ERR, "Invalid queue size (%d)",
                  child_counter);
}

void dp_status_timeout(time_t timestamp)
{
    /* process possible timeouts */
    for (size_t i = 0; i < child_limit; ++i)
        if (!child_status[i].null &&
            child_status[i].stamp < timestamp) {

            /* check whether we already send termination signal */
            if (child_status[i].stamp == 0)
                continue;

            /* update status to indicate timeout termination */
            child_status[i].stamp = 0;

            /* terminate child */
            /* NOTE: parent handlers are used because of fork */
            kill(child_status[i].pid, SIGTERM);
        }
}

dp_child *dp_child_null()
{
    for (size_t i = 0; i < child_limit; ++i)
        if (child_status[i].null)
            return &child_status[i];
    return NULL;
}

dp_child *dp_child_pid(pid_t pid)
{
    for (size_t i = 0; i < child_limit; ++i)
        if (!child_status[i].null)
            if (child_status[i].pid == pid)
                return &child_status[i];
    return NULL;
}

