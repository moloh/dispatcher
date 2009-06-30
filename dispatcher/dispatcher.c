#include "dispatcher.h"

/* TODO: portable printf format for pid_t (now %d) */

int main()
{
    int32_t queue_counter = child_counter; /* number of jobs in queue */
    int32_t sense_counter = 0;             /* number of empty iterations (queue not full) */
    int32_t terminate_counter = -1;        /* number of terminate iterations */
    int32_t pause_counter = 0;             /* number of pause iterations */
    char query[QUERY_LIMIT];               /* query buffer */
    MYSQL *db = NULL;

    /* initialize signal and status processing */
    if (!dp_config_init() ||
        !dp_status_init() ||
        !dp_signal_init())
        return EXIT_FAILURE;

    /* initialize logger */
    dp_logger_init(DP_PARENT);

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

    /* main loop */
    while (TRUE) {
        time_t timestamp;
        bool is_job = FALSE;
        dp_task task;
        pid_t pid;

        /* Check if we should reload configuration */
        if (reload_flag == TRUE) {
            dp_logger(LOG_WARNING, "Reloading configuration...");
            fprintf(stderr, "Reloading configuration...""\n");

            /* reload configuration
             * NOTE: this function automatically merges configuration and
             *       handles errors
             */
            dp_config_init();

            reload_flag = FALSE;
        }

        /* get timestamp for current iteration */
        timestamp = time(NULL);

        /* If we are already at maximum fork capacity, we shouldn't
         * grab another task, we should just sleep for a bit.  Or,
         * maybe we were told to stop dispatching or terminate.
         *
         * NOTE: this prevents automatic update of task in database, even
         * when there is no space in queue
         *
         * NOTE: pause sense counters are reset when flag is disabled
         */
        if ((terminate_flag == TRUE) ||
            (pause_flag == TRUE) ||
            (queue_counter >= QUEUE_LIMIT)) {

            /* Check if we should terminate */
            if (terminate_flag == TRUE) {
                /* if we start countdown then we should print some information
                 * to syslog and terminal
                 */
                if (terminate_counter < 0) {
                    dp_logger(LOG_WARNING, "Terminating... Waiting for children");
                    fprintf(stderr, "Terminating... Waiting for children""\n");
                    terminate_counter = 0;
                }

                /* Check if we should print sense information during terminate */
                if (cfg.sense.terminated &&
                    terminate_counter++ >= cfg.sense.terminated) {

                    dp_logger(LOG_WARNING, "(%d/%d) Terminating...", queue_counter, QUEUE_LIMIT);
                    fprintf(stderr, "(%d/%d) Terminating...""\n", queue_counter, QUEUE_LIMIT);
                    terminate_counter = 0;
                }

                /* Check if all children are done
                 * NOTE: we break main loop here
                 */
                if (queue_counter <= 0) {
                    dp_logger(LOG_WARNING, "Terminated");
                    fprintf(stderr, "Terminated""\n");
                    break;
                }
            }

            /* Check if we are paused */
            if (pause_flag == TRUE) {
                /* Check if we should print sense information during pause */
                if (cfg.sense.paused &&
                    ++pause_counter >= cfg.sense.paused) {

                    dp_logger(LOG_NOTICE, "(%d/%d) Sleeping...", queue_counter, QUEUE_LIMIT);
                    pause_counter = 0;
                }
            } else
                pause_counter = 0;

            /* This sleep call may be interrupted by a signal, but the
             * only signals we care about are when a child is finished,
             * at which point, we want to try another task anyway.
             */
            sleep(cfg.sleep_loop);

            /* update status of workers and get number of workers */
            dp_status_timeout(timestamp - cfg.delay.task_timeout, &queue_counter);
            dp_status_update(&queue_counter);

            continue;
        } else
            pause_counter = 0;

        /* START fake loop for query "exceptions" */
        do {
            MYSQL_RES *result;
            int id = -1;

            /* make double sure that there is space in queue for task,
             * we don't want to make it 'working' and do nothing later
             */
            if (queue_counter >= QUERY_LIMIT)
                break;

            /* reset counter */
            if (!dp_mysql_query(db, "SET @id := NULL"))
                break;

            /* update task to 'working' state and extract its id */
            snprintf(query, QUERY_LIMIT,
                     "UPDATE %s SET status = 'working', run_after = '%ld' "
                     "WHERE id = "
                     "(SELECT * FROM (SELECT id FROM %s "
                        "WHERE status IN ('new','working') AND run_after < %ld "
                        "ORDER BY priority DESC LIMIT 1) AS innerquery) "
                     "AND @id := id",
                     cfg.mysql.table, timestamp + cfg.delay.task_timeout,
                     cfg.mysql.table, timestamp);
            if (!dp_mysql_query(db, query))
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
            snprintf(query, QUERY_LIMIT,
                     "SELECT * FROM %s WHERE id = %d",
                     cfg.mysql.table, id);
            if (!dp_mysql_query(db, query))
                break;

            result = mysql_store_result(db);
            if (!dp_mysql_get_task(&task, result))
                dp_logger(LOG_ERR, "Failed to get job (%d) details!", id);
            else
                is_job = TRUE;
            mysql_free_result(result);

        /* END fake loop for query "exceptions" */
        } while (FALSE);

        /* update basic counters */
        if (!is_job)
            sense_counter += 1;
        else
            sense_counter = 0;

        if (cfg.sense.loop &&
            sense_counter >= cfg.sense.loop) {

            dp_logger(LOG_NOTICE, "(%d/%d) ...", queue_counter, QUEUE_LIMIT);
            sense_counter = 0;
        }

        /* START fake loop for dispatching "exceptions" */
        do {
            dp_child *worker;

            /* check if we have free spots
             * NOTE: currently is there is job we get here only if there is
             * free spot in queue, but check of queue size anyway
             */
            if (!is_job || queue_counter >= QUEUE_LIMIT)
                break;

            /* find first empty entry */
            if ((worker = dp_child_null()) == NULL) {
                dp_logger(LOG_ERR, "Inconsistence in dispatching job (%d): No more free workers",
                          task.id);
                break;
            }

            /* initialize worker data */
            worker->task = task;
            worker->stamp = timestamp;
            worker->null = FALSE;

            /* fork worker */
            if ((pid = fork()) == 0) { /* child */
                /* NOTE: child does not manage memory or free resources */

                gearman_client_st *client = NULL;
                gearman_return_t error;
                dp_reply reply;
                void *worker_result = NULL;
                size_t worker_result_size;
                const char *value;

                /* initialize logger */
                dp_logger_init(DP_CHILD);

                dp_logger(LOG_DEBUG, "Worker forked (%d/%d) job (%d)",
                          queue_counter + 1, QUEUE_LIMIT, worker->task.id);

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

                /* get result timestamp */
                timestamp = time(NULL);

                /* NOTE: we initialize mysql only after gearman finished work
                 *       this prevents timeouts from mysql connection but may
                 *       prove problematic.  The task is in 'working' status,
                 *       and when the task completes (or fails) and if we
                 *       cannot connect to MySQL, we can't update the status.
                 */

                /* initialize mysql for worker */
                my_init();

                /* erase parent connection data */
                memset(db, 0, sizeof(MYSQL));
                if (!dp_mysql_init(&db) ||
                    !dp_mysql_connect(db))
                    return EXIT_FAILURE;

                /* error executing work, retry */
                if (error) {
                    dp_logger(LOG_ERR, "Worker job (%d) FAILED (%d)",
                              worker->task.id, error);

                    /* prepare query */
                    snprintf(query, QUERY_LIMIT,
                             "UPDATE %s "
                             "SET status = 'new', result = '' "
                             "WHERE id = %d",
                             cfg.mysql.table,
                             worker->task.id);

                    /* execute query */
                    if (!dp_mysql_query(db, query))
                        return EXIT_FAILURE;
                    return error;
                }

                /* process reply from gearman */
                /* NOTE: reply may contain non-escaped characters */
                dp_gearman_get_reply(&reply, worker_result, worker_result_size);

                /* escape value that we want to write to MySQL */
                dp_gearman_reply_escape(&reply, DP_REPLY_STATUS);
                value = dp_gearman_reply_value(&reply, DP_REPLY_STATUS);

                /* prepare query to database */
                if (value != NULL && !strcmp(value, ":ok")) {

                    /* limit query result length to QUERY_LIMIT - 512 */
                    /* TODO: limiting result may make escape invalid */
                    snprintf(query, QUERY_LIMIT,
                             "UPDATE %s "
                             "SET status = 'done', result = '%.*s', result_timestamp = '%ld' "
                             "WHERE id = %d",
                             cfg.mysql.table,
                             QUERY_LIMIT - 512, value, timestamp,
                             worker->task.id);

                } else {
                    dp_logger(LOG_ERR, "Worker job (%d) result is NOT ok (%s)",
                              worker->task.id, value);

                    snprintf(query, QUERY_LIMIT,
                             "UPDATE %s "
                             "SET status = 'new', result = '%.*s', run_after = '%ld' "
                             "WHERE id = %d",
                             cfg.mysql.table,
                             QUERY_LIMIT - 512, value, timestamp + cfg.delay.task_failed,
                             worker->task.id);
                }

                /* execute query */
                if (!dp_mysql_query(db, query))
                    return EXIT_FAILURE;

                /* close mysql connection */
                mysql_close(db);

                dp_logger(LOG_DEBUG, "Worker finished");
                return EXIT_SUCCESS;

            } /* end child */

            /* detect fork error */
            /* fork failed */
            if (pid < 0) {
                dp_logger(LOG_ERR, "Fork failed!");

                snprintf(query, QUERY_LIMIT,
                         "UPDATE %s "
                         "SET status = 'new', result = ':fork', run_after = '%ld' "
                         "WHERE id = %d",
                         cfg.mysql.table,
                         timestamp,
                         worker->task.id);

                /* execute query */
                if (!dp_mysql_query(db, query))
                    return EXIT_FAILURE;

            /* fork successful */
            } else {
                /* update worker status */
                worker->pid = pid;

                /* increase task count */
                child_counter += 1;
                queue_counter += 1;
            }
        /* END fake loop for dispatching "exceptions" */
        } while (FALSE);

        /* wait for next iteration
         * NOTE: sleep terminates when we receive signal
         * NOTE: sleep only when no task are waiting or we have full queue
         */
        if (!is_job || (is_job && queue_counter >= QUEUE_LIMIT))
            sleep(cfg.sleep_loop);

        /* update status of workers and get number of running workers */
        dp_status_timeout(timestamp - cfg.delay.task_timeout, &queue_counter);
        dp_status_update(&queue_counter);
    }

    /* close mysql connection */
    mysql_close(db);

    /* free configuration resources */
    dp_config_free(&cfg);

    return EXIT_SUCCESS;
}

/* Load dispatcher configuration file, can be called multiple times.
 * When configuration file is invalid then function return FALSE;
 */
bool dp_config_init()
{
    FILE *fconfig;
    char buffer[BUFFER_LIMIT];
    char value[BUFFER_LIMIT];
    char name[BUFFER_LIMIT];
    dp_config_val field;
    uint32_t line;
    bool is_eof = FALSE, is_error = FALSE;
    dp_config config;

    /* initialize new config */
    memset(&config, 0, sizeof(dp_config));

    /* open configuration file */
    fconfig = fopen(DP_CONFIG"/dispatcher.conf", "r");
    if (fconfig == NULL)
        return FALSE;

    /* read each line separately */
    for (line = 1; fgets(buffer, BUFFER_LIMIT, fconfig); ++line) {

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
        if (!dp_config_set(&config, field, value, TRUE)) {
            is_error = TRUE;
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
        dp_logger(LOG_ERR, "Invalid configuration file at line (%"SCNu32")", line);
        fprintf(stderr, "Invalid configuration file at line (%"SCNu32")\n", line);
        return FALSE;
    }

    return TRUE;
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

    return TRUE;
}

bool dp_signal_block(sigset_t *old)
{
    sigset_t mask;

    /* initialize masks */
    sigemptyset(old);
    sigemptyset(&mask);
    sigaddset(&mask, SIGCHLD);

    /* apply block */
    sigprocmask(SIG_BLOCK, &mask, old);

    return TRUE;
}

bool dp_signal_restore(sigset_t *restore)
{
    /* restore mask */
    sigprocmask(SIG_SETMASK, restore, NULL);

    return TRUE;
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
    if (!strcmp(name, "delay_task_failed"))
        return DP_CONFIG_DELAY_TASK_FAILED;
    if (!strcmp(name, "delay_task_timeout"))
        return DP_CONFIG_DELAY_TASK_TIMEOUT;
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
    switch (field) {
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
            return FALSE;
        break;
    case DP_CONFIG_GEARMAN_HOST:
        if (config->gearman.host) free(config->gearman.host);
        if (!if_dup) config->gearman.host = value;
        else config->gearman.host = dp_strdup(value);
        break;
    case DP_CONFIG_GEARMAN_PORT:
        if (sscanf(value, "%d", &config->gearman.port) != 1)
            return FALSE;
        break;
    case DP_CONFIG_DELAY_TASK_FAILED:
        if (sscanf(value, "%" SCNu16, &config->delay.task_failed) != 1)
            return FALSE;
        break;
    case DP_CONFIG_DELAY_TASK_TIMEOUT:
        if (sscanf(value, "%" SCNu16, &config->delay.task_timeout) != 1)
            return FALSE;
        break;
    case DP_CONFIG_SENSE_LOOP:
        if (sscanf(value, "%" SCNu16, &config->sense.loop) != 1)
            return FALSE;
        break;
    case DP_CONFIG_SENSE_TERMINATED:
        if (sscanf(value, "%" SCNu16, &config->sense.terminated) != 1)
            return FALSE;
        break;
    case DP_CONFIG_SENSE_PAUSED:
        if (sscanf(value, "%" SCNu16, &config->sense.paused) != 1)
            return FALSE;
        break;
    case DP_CONFIG_SLEEP_LOOP:
        if (sscanf(value, "%" SCNu16, &config->sleep_loop) != 1)
            return FALSE;
        break;
    default:
        return FALSE;
    }

    return TRUE;
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
    }
}

/* basic, logged initialization of gearman */
bool dp_gearman_init(gearman_client_st **client)
{
    if ((*client = gearman_client_create(NULL)) == NULL) {
        dp_logger(LOG_ERR, "Gearman initialization failed");
        return FALSE;
    }

    if (gearman_client_add_server(*client, cfg.gearman.host,
                                           cfg.gearman.port)) {
        dp_logger(LOG_ERR,
                  "Gearman connection error (%d): %s",
                  gearman_client_errno(*client),
                  gearman_client_error(*client));
        return FALSE;
    }

    dp_logger(LOG_INFO, "Gearman connection established");
    return TRUE;
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
                return FALSE;
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

    return FALSE;
}

/* set single "hash" value from gearman parser */
bool dp_gearman_reply_set(dp_reply *reply, dp_reply_val field, char *value)
{
    switch (field) {
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
    default:
        return FALSE;
    }

    return TRUE;
}

bool dp_gearman_reply_escape(dp_reply *reply, dp_reply_val field)
{
    char **value = NULL;
    char *escape = NULL, *str, *esc;
    size_t size = 0;

    switch (field) {
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
    default:
        return FALSE;
    }

    /* check if there is data to escape */
    if (*value == NULL)
        return TRUE;

    /* check number of characters to escape */
    for (str = *value; *str; ++str)
        switch (*str) {
        case '\\':
        case '\'':
            size += 1;
        }

    /* string length (remember about null terminator) */
    size += str - *value + 1;

    /* allocate required escape buffer */
    if ((escape = malloc(size)) == NULL)
        return FALSE;

    /* escape string */
    for (str = *value, esc = escape; *str; ++str, ++esc)
        switch (*str) {
        case '\\':
        case '\'':
            *esc++ = '\\';
        default:
            *esc = *str;
        }

    /* finalize string */
    *esc = '\0';

    /* set new escaped version of string */
    free(*value);
    *value = escape;

    return TRUE;
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
        return FALSE;
    }

    return TRUE;
}

/* reusable logged MySQL connect function */
bool dp_mysql_connect(MYSQL *db)
{
    /* check for valid MySQL structure */
    if (!db) return FALSE;

    /* try open connection */
    if (!mysql_real_connect(db, cfg.mysql.host,
                                cfg.mysql.user,
                                cfg.mysql.passwd,
                                cfg.mysql.db,
                                cfg.mysql.port, NULL, /* port, socket */
                                0)) {                 /* client flags */
        dp_logger(LOG_ERR, "MySQL connection error: %s", mysql_error(db));
        return FALSE;
    }

    dp_logger(LOG_INFO, "MySQL connection established");
    return TRUE;
}

/* logged MySQL query wrapper with basic recovering */
bool dp_mysql_query(MYSQL *db, const char *query)
{
    MYSQL_RES *result;

    /* check if we have query at all */
    if (query == NULL) {
        dp_logger(LOG_ERR, "MySQL query is missing!");
        return FALSE;
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
                return FALSE;
            case CR_SERVER_GONE_ERROR:     /* try to reconnect and rerun query */
            case CR_SERVER_LOST:
                dp_logger(LOG_ERR, "MySQL query error (%d), recovering: %s",
                          mysql_errno(db),
                          mysql_error(db));

                /* reconnect to server */
                if (!dp_mysql_connect(db))
                    return FALSE;

                /* execute query, recover */
                if (mysql_query(db, query)) {
                    dp_logger(LOG_ERR, "MySQL query recovery error (%d): %s",
                              mysql_errno(db),
                              mysql_error(db));

                    /* clear state anyway */
                    result = mysql_store_result(db);
                    mysql_free_result(result);
                    return FALSE;
                }

                return TRUE;
            default:
                return FALSE;
        }
    }

    return TRUE;
}

/* TODO: error checking for mysql_fetch_row */
/* convert single row into typed task */
bool dp_mysql_get_task(dp_task *task, MYSQL_RES *result)
{
    char buffer[BUFFER_LIMIT];
    MYSQL_FIELD *field;
    MYSQL_ROW row;
    size_t size;

    /* init task structure */
    memset(task, 0, sizeof(dp_task));

    if (result == NULL)
        return FALSE;

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

    /* process id */
    snprintf(buffer, BUFFER_LIMIT, "\n---\n:task_id: \"%d\"", task->id);

    /* postprocess fields, allocate strings */
    if (task->type)
        task->type = dp_strdup(task->type);
    if (task->description)
        task->description = dp_strdup(task->description);
    if (task->status)
        task->status = dp_strdup(task->status);

    return TRUE;
}

bool dp_mysql_get_int(int *value, MYSQL_RES *result)
{
    MYSQL_ROW row;

    if (result == NULL)
        return FALSE;

    /* fetch single result */
    row = mysql_fetch_row(result);

    /* validate row */
    if (mysql_num_rows(result) == 1 && mysql_num_fields(result) == 1)
        if (row[0] != NULL)
            sscanf(row[0], "%d", value);

    return TRUE;
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

void dp_logger_init(const char *ident)
{
    closelog();
    openlog(ident, LOG_PID, LOG_DAEMON);
}

void dp_logger(int priority, const char *message, ...)
{
    va_list args;

    va_start(args, message);
    vsyslog(priority, message, args);
    va_end(args);
}

int dp_asprintf(char **str, const char *format, ...)
{
    va_list arg;
    int retval = -1;

    va_start(arg, format);

    /* initialize result */
    *str = NULL;

#ifdef HAVE_ASPRINTF
    retval = vasprintf(str, format, arg);
#else
    char character;
    int size;

    /* get size of string */
    if ((size = vsnprintf(&character, 1, format, arg)) < 0)
        goto retval;

    /* allocate memory */
    if ((*str = malloc(size+1)) == NULL)
        goto retval;

    if ((retval = vsprintf(*str, format, arg)) < 0) {
        free(*str); *str = NULL;
        goto retval;
    }
#endif

retval:
    va_end(arg);
    return retval;
}

void dp_sigchld(int signal)
{
    /* update flag to note pending processing */
    child_flag = TRUE;
}

void dp_sighup(int signal)
{
    /* dispatcher should reload configuration */
    reload_flag = TRUE;
}

void dp_sigtermint(int signal)
{
    /* dispatcher should terminate */
    terminate_flag = TRUE;
}

void dp_sigusr12(int signal)
{
    /* Toggle flag to pause or resume dispatching */
    if (pause_flag == TRUE)
        pause_flag = FALSE;
    else
        pause_flag = TRUE;
}

bool dp_status_init()
{
    /* initialize status array */
    for (size_t i = 0; i < QUEUE_LIMIT; ++i) {
        /* clear task */
        dp_mysql_task_clear(&child_status[i].task);

        /* clear other info */
        child_status[i].pid = 0;
        child_status[i].null = TRUE;
    }

    return TRUE;
}

void dp_status_update(int32_t *queue_counter)
{
    int status;
    pid_t pid;

    /* update queue size if needed */
    if (queue_counter != NULL)
        *queue_counter = child_counter;

    /* check if we have pending events */
    if (!child_flag)
        return;

    while ((pid = waitpid(0, &status, WNOHANG)) > 0) {
        dp_child *worker = NULL;
        bool is_end = FALSE;

        /* NOTE: we update flag only after successful retrieval of reaped
         * process
         */

        /* update flag */
        child_flag = FALSE;

        /* extract information about specific child */
        if ((worker = dp_child_pid(pid)) == NULL) {
            dp_logger(LOG_ERR, "Unknown child pid (%d)!",
                      pid);
            continue;
        }

        /* check if normal exit */
        if (WIFEXITED(status)) {
            child_counter -= 1;
            is_end = TRUE;

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
                if (worker->stamp == 0)
                    dp_logger(LOG_ERR,
                              "Child worker (%d) timeout (job -> %d)",
                              worker->pid, worker->task.id);
                else
                    dp_logger(LOG_ERR,
                              "Child worker (%d) terminated (job -> %d)",
                              worker->pid, worker->task.id);

                child_counter -= 1;
                is_end = TRUE;
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
            worker->null = TRUE;
        }
    }

    /* validate size of queue */
    if (child_counter < 0)
        dp_logger(LOG_ERR, "Invalid queue size (%d)",
                  child_counter);

    /* update queue size if needed
     * NOTE: value might changed (and probably did)
     */
    if (queue_counter != NULL)
        *queue_counter = child_counter;
}

#include <errno.h>

void dp_status_timeout(time_t timestamp, int32_t *queue_counter)
{
    /* process possible timeouts */
    for (size_t i = 0; i < QUEUE_LIMIT; ++i)
        if (!child_status[i].null &&
            child_status[i].stamp < timestamp) {

            /* check whether we already send termination signal */
            if (child_status[i].stamp == 0)
                continue;

            /* update status to indicate timeout termination */
            child_status[i].stamp = 0;

            /* terminate child */
            /* NOTE: parent handlers are used because of fork */
            kill(child_status[i].pid, SIGKILL);
        }

    /* update queue size if needed */
    if (queue_counter != NULL)
        *queue_counter = child_counter;
}

dp_child *dp_child_null()
{
    for (size_t i = 0; i < QUEUE_LIMIT; ++i)
        if (child_status[i].null)
            return &child_status[i];
    return NULL;
}

dp_child *dp_child_pid(pid_t pid)
{
    for (size_t i = 0; i < QUEUE_LIMIT; ++i)
        if (!child_status[i].null)
            if (child_status[i].pid == pid)
                return &child_status[i];
    return NULL;
}

