/* standard headers */
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdbool.h>  /* _Bool */
#include <unistd.h>   /* sleep, fork */
#include <signal.h>   /* SIGCHLD handling */
#include <sys/wait.h> /* wait */
#include <time.h>     /* time */
#include <ctype.h>    /* isspace */

/* other headers */
#include <mysql/mysql.h>        /* mysql */
#include <mysql/errmsg.h>
#include <libgearman/gearman.h> /* gearman */
#include <syslog.h>             /* system log */

/* global defines */
#define QUEUE_LIMIT        10    /* maximum number of concurrent workers */
#define QUERY_LIMIT        4096  /* maximum MySQL query length */
#define BUFFER_LIMIT       1024  /* maximal length of internal buffers */
#define SENSE_LIMIT        30    /* sense log delay */
#define SLEEP_TIMEOUT      1     /* main loop timeout */
#define TIMESTAMP_DELAY    5*60  /* task execution delay */

/* syslog identifiers */
#define DP_PARENT       "dispatcher"
#define DP_CHILD        "worker"

/* mysql server info */
#define DP_MYSQL_HOST   "192.168.10.89"
#define DP_MYSQL_USER   "root"
#define DP_MYSQL_PASSWD "Ohio"
#define DP_MYSQL_DB     "processqueue_dev"
#define DP_MYSQL_PORT   0
#define DP_MYSQL_TABLE  "deferred_tasks_new"

/* gearman server info */
#define DP_GEARMAN_HOST "192.168.10.51"
#define DP_GEARMAN_PORT 7003

/* internal bool */
typedef _Bool dp_bool;
#define TRUE  (_Bool)1
#define FALSE (_Bool)0

#if 0
#undef LOG_WARNING
#undef LOG_DEBUG
#undef LOG_INFO
#define LOG_WARNING LOG_ERR
#define LOG_DEBUG LOG_ERR
#define LOG_INFO LOG_ERR
#endif

/* task definition structure */
typedef struct dp_task {
    int id;
    int priority;
    char *type;
    char *description;
    char *status;
    time_t run_after;
} dp_task;

/* child definition structure */
typedef struct dp_child {
    pid_t pid;      /* pid of child */
    dp_task task;   /* task associated with child */
    dp_bool null;   /* indicate empty entry */
} dp_child;

/* task reply definition structure */
typedef struct dp_task_reply {
    char *backtrace;
    char *error;
    char *status;
    char *result;
    char *message;
} dp_task_reply;

/* global signal flag */
volatile sig_atomic_t child_flag = FALSE;

/* global status variables */
int      child_counter = 0;         /* current number of running children */
dp_child child_status[QUEUE_LIMIT]; /* array of child status */

/* internal functions */
dp_bool dp_signal_init    ();                  /* initialize signal handling (logged) */
dp_bool dp_signal_block   (sigset_t *old);     /* block SIGCHLD and return old mask */
dp_bool dp_signal_restore (sigset_t *restore); /* restore old mask */

dp_bool dp_gearman_init      (gearman_client_st **client);                            /* initialize gearman (logged) */
dp_bool dp_gearman_get_reply (dp_task_reply *reply, const char *result, size_t size); /* parse gearman reply */
dp_bool dp_gearman_get_value (dp_task_reply *reply, const char *name, char *value);   /* assign name value to reply */

dp_bool dp_mysql_init       (MYSQL **db);                              /* initialize MySQL (logged) */
dp_bool dp_mysql_connect    (MYSQL *db);                               /* connect to MySQL (logged) */
dp_bool dp_mysql_query      (MYSQL *db, const char *query);            /* execute MySQL query (logged), recover */
dp_bool dp_mysql_get_task   (dp_task *task, MYSQL_RES *result);        /* extract MySQL stored task (logged) */
dp_bool dp_mysql_get_int    (int *value, MYSQL_RES *result);           /* extract MySQL int variable (logged) */
void    dp_mysql_task_free  (dp_task *task);                           /* free data associated with task */
void    dp_mysql_task_clear (dp_task *task);                           /* clear data associated with task */

void    dp_logger_init   (const char *ident);                                  /* initialize logging capabilities */
void    dp_logger        (int priority, const char *message, ...);             /* log message with specific priority */
int     dp_asprintf      (char **strp, const char *format, ...);               /* portability wrapper, allocated sprintf */
char   *dp_strdup        (const char *str);                                    /* dup string helper */
char   *dp_strudup       (const char *str, size_t length);                     /* sized dup string helper */
char   *dp_struchr       (const char *str, size_t length, char character);     /* sized strchr string helper */
char   *dp_strustr       (const char *str, size_t length, const char *locate); /* sized strstr string helper */
char   *dp_strcat        (const char *str, ...);                               /* concatenate string helper */
void    dp_sigchld       (int signal);                                         /* SIGCHLD handler */
void    dp_status_update ();                                                   /* process child_status table */

dp_child *dp_child_null ();           /* find first null entry in child_status array */
dp_child *dp_child_pid  (pid_t pid);  /* find child with pid in child_status array */

int main()
{
    size_t queue_counter = 0;             /* number of jobs in queue */
    size_t sense_counter = 0;             /* number of empty iterations */
    char query[QUERY_LIMIT];              /* query buffer */
    MYSQL *db = NULL;

    /* initialize signal processing */
    if (!dp_signal_init())
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
        dp_bool is_job = FALSE;
        dp_task task;
        pid_t pid;

        /* extract current number of child workers */
        queue_counter = child_counter;

        /* If we are already at maximum fork capacity, we shouldn't
         * grab another task, we should just sleep for a bit
         */
        if (queue_counter >= QUEUE_LIMIT) {
            /* This sleep call may be interrupted by a signal, but the
             * only signals we care about are when a child is finished,
             * at which point, we want to try another task anyway.
             */
            sleep(SLEEP_TIMEOUT);

            /* update status of workers */
            dp_status_update();

            /* update number of workers */
            queue_counter = child_counter;

            continue;
        }

        /* get timestamp */
        timestamp = time(NULL);

        /* START fake loop for query "exceptions" */
        do {
            MYSQL_RES *result;
            int id = -1;

            /* reset counter */
            if (!dp_mysql_query(db, "SET @id := NULL"))
                break;

            /* update task and extract its id */
            snprintf(query, QUERY_LIMIT,
                     "UPDATE "DP_MYSQL_TABLE" SET status = 'working' "
                     "WHERE id = "
                     "(SELECT * FROM (SELECT id FROM "DP_MYSQL_TABLE" "
                        "WHERE status = 'new' AND run_after < %ld "
                        "ORDER BY priority DESC LIMIT 1) AS innerquery) "
                     "AND @id := id",
                     timestamp);
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

            /* check if we get valid reply */
            if (id < 0)
                break;

            /* extract task */
            snprintf(query, QUERY_LIMIT,
                     "SELECT * FROM "DP_MYSQL_TABLE" WHERE id = %d",
                     id);
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
        if (!is_job) {
            sense_counter += 1;
        } else
            sense_counter = 0;

        if (sense_counter >= SENSE_LIMIT) {
            dp_logger(LOG_NOTICE, "(%d/%d) ...", queue_counter, QUEUE_LIMIT);
            sense_counter = 0;
        }

        if (is_job && queue_counter < QUEUE_LIMIT) {
            /* find first empty entry */
            dp_child *worker = dp_child_null();

            /* initialize worker data */
            worker->task = task;
            worker->null = FALSE;

            /* fork worker */
            if ((pid = fork()) == 0) { /* child */
                /* NOTE: child does not manage memory or free resources */

                gearman_client_st *client = NULL;
                gearman_return_t error;
                dp_task_reply reply;
                void *worker_result = NULL;
                size_t worker_result_size;

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
                             "UPDATE "DP_MYSQL_TABLE" "
                             "SET status = 'new', result = '' "
                             "WHERE id = %d",
                             worker->task.id);

                    /* execute query */
                    if (!dp_mysql_query(db, query))
                        return EXIT_FAILURE;
                    return error;
                }

                /* NOTE: reply may contain non-escaped characters */

                /* process reply from gearman */
                dp_gearman_get_reply(&reply, worker_result, worker_result_size);

                /* prepare query to database */
                if (reply.status != NULL &&
                    !strcmp(reply.status, ":ok")) {

                    snprintf(query, QUERY_LIMIT,
                             "UPDATE "DP_MYSQL_TABLE" "
                             "SET status = 'done', result = '%s', result_timestamp = '%ld' "
                             "WHERE id = %d",
                             reply.status, timestamp, worker->task.id);

                } else {
                    dp_logger(LOG_ERR, "Worker job (%d) result is NOT ok (%s)",
                              worker->task.id, reply.status);

                    snprintf(query, QUERY_LIMIT,
                             "UPDATE "DP_MYSQL_TABLE" "
                             "SET status = 'new', run_after = '%ld', result = '%s' "
                             "WHERE id = %d",
                             timestamp + TIMESTAMP_DELAY, reply.status, worker->task.id);
                }

                /* execute query */
                if (!dp_mysql_query(db, query))
                    return EXIT_FAILURE;

                dp_logger(LOG_DEBUG, "Worker finished");
                return EXIT_SUCCESS;
            }

            /* detect fork error */
            /* fork failed */
            if (pid < 0) {
                /* TODO: update database */
                dp_logger(LOG_ERR, "Fork failed!");
            /* fork successful */
            } else {
                /* update worker status */
                worker->pid = pid;
                /* increase task count */
                child_counter += 1;
            }
        }

        /* wait for next iteration */
        /* NOTE: sleep terminates when we receive signal */
        /* NOTE: sleep only when no task are waiting or we have full queue */

        if (!is_job || (is_job && queue_counter >= QUEUE_LIMIT))
            for (uint timeout = SLEEP_TIMEOUT; timeout > 0;)
                timeout = sleep(timeout);

        /* update status of workers */
        dp_status_update();
    }

    mysql_close(db);
    return EXIT_SUCCESS;
}

dp_bool dp_signal_init()
{
    /* initialize status array */
    for (size_t i = 0; i < QUEUE_LIMIT; ++i) {
        /* clear task */
        dp_mysql_task_clear(&child_status[i].task);

        /* clear other info */
        child_status[i].pid = 0;
        child_status[i].null = TRUE;
    }

    /* setup signals */
    struct sigaction action;
    sigset_t block;

    /* queue multiple SIGCHLD signals */
    sigemptyset(&block);
    sigaddset(&block, SIGCHLD);

    /* setup action */
    action.sa_handler = dp_sigchld;
    action.sa_mask = block;
    action.sa_flags = 0;

    /* install signal handler */
    sigaction(SIGCHLD, &action, NULL);

    return TRUE;
}

dp_bool dp_signal_block(sigset_t *old)
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

dp_bool dp_signal_restore(sigset_t *restore)
{
    /* restore mask */
    sigprocmask(SIG_SETMASK, restore, NULL);

    return TRUE;
}

/* basic, logged initialization of gearman */
dp_bool dp_gearman_init(gearman_client_st **client)
{
    if ((*client = gearman_client_create(NULL)) == NULL) {
        dp_logger(LOG_ERR, "Gearman initialization failed");
        return FALSE;
    }

    if (gearman_client_add_server(*client, DP_GEARMAN_HOST,
                                           DP_GEARMAN_PORT)) {
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
/* TODO: add escaping of reply fields */

/* simple "parser" for YAML reply from gearman */
dp_bool dp_gearman_get_reply(dp_task_reply *reply, const char *result, size_t size)
{
    const char *str = result, *end;
    const char *last = result + strlen(result);
    char *name = NULL;

    /* initialize */
    memset(reply, 0, sizeof(dp_task_reply));

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
                if (!dp_gearman_get_value(reply, name, value))
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
        if (!dp_gearman_get_value(reply, name, value))
            free(value);
        free(name);
    }

    return FALSE;
}

/* set single "hash" value from gearman parser */
dp_bool dp_gearman_get_value (dp_task_reply *reply, const char *name, char *value)
{
    if (name == NULL)
        return FALSE;

    if (!strcmp(name, ":backtrace:")) {
        if (reply->backtrace) free(reply->backtrace);
        reply->backtrace = value;
    } else if (!strcmp(name, ":error:")) {
        if (reply->error) free(reply->error);
        reply->error = value;
    } else if (!strcmp(name, ":status:")) {
        if (reply->status) free(reply->status);
        reply->status = value;
    } else if (!strcmp(name, ":result:")) {
        if (reply->result) free(reply->result);
        reply->result = value;
    } else if (!strcmp(name, ":message:")) {
        if (reply->message) free(reply->message);
        reply->message = value;
    } else
        return FALSE;

    return TRUE;
}

/* basic, logged initialization of MySQL */
dp_bool dp_mysql_init(MYSQL **db)
{
    if ((*db = mysql_init(*db)) == NULL) {
        dp_logger(LOG_ERR, "MySQL initialization error");
        return FALSE;
    }

    return TRUE;
}

/* reusable logged MySQL connect function */
dp_bool dp_mysql_connect(MYSQL *db)
{
    /* check for valid MySQL structure */
    if (!db) return FALSE;

    /* try open connection */
    if (!mysql_real_connect(db, DP_MYSQL_HOST,
                                DP_MYSQL_USER,
                                DP_MYSQL_PASSWD,
                                DP_MYSQL_DB,
                                DP_MYSQL_PORT, NULL, /* port, socket */
                                0)) {             /* client flags */
        dp_logger(LOG_ERR, "MySQL connection error: %s", mysql_error(db));
        return FALSE;
    }

    dp_logger(LOG_INFO, "MySQL connection established");
    return TRUE;
}

/* logged MySQL query wrapper with basic recovering */
dp_bool dp_mysql_query(MYSQL *db, const char *query)
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
dp_bool dp_mysql_get_task(dp_task *task, MYSQL_RES *result)
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

dp_bool dp_mysql_get_int(int *value, MYSQL_RES *result)
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

void dp_status_update()
{
    int status;
    pid_t pid;

    /* check if we have pending events */
    if (!child_flag)
        return;

    while ((pid = waitpid(0, &status, WNOHANG)) > 0) {
        dp_child *worker = NULL;
        dp_bool is_end = FALSE;

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
                          "Child worker (%d) invalid exit (%d)",
                          worker->pid, WEXITSTATUS(status));

        /* check type of signal */
        } else if (WIFSIGNALED(status)) {
            if (WTERMSIG(status) || WCOREDUMP(status)) {
                dp_logger(LOG_ERR,
                          "Child worker (%d) terminated",
                          worker->pid);
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

