/* standard headers */
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdbool.h>  /* _Bool */
#include <unistd.h>   /* sleep, fork */
#include <signal.h>   /* SIGCHILD handling */
#include <sys/wait.h> /* wait */
#include <time.h>     /* time */

/* other headers */
#include <mysql/mysql.h>        /* mysql */
#include <mysql/errmsg.h>
#include <libgearman/gearman.h> /* gearman */
#include <syslog.h>             /* system log */

/* global defines */
#define PENDING_FULL_LIMIT 60    /* log overload */
#define QUEUE_LIMIT        10    /* maximum number of concurrent workers */
#define QUERY_LIMIT        1024  /* maximum MySQL query length */
#define BUFFER_LIMIT       1024  /* maximul length of internal buffers */
#define SENSE_LIMIT        30    /* sense log delay */
#define SLEEP_TIMEOUT      1     /* main loop timeout */
#define TIMESTAMP_DELAY    5*60  /* task execution delay */

/* mysql server info */
#define DP_MYSQL_HOST   "192.168.10.89"
#define DP_MYSQL_USER   "root"
#define DP_MYSQL_PASSWD "Ohio"
#define DP_MYSQL_DB     "processqueue_dev"
#define DP_MYSQL_PORT   0

/* gearman server info */
#define DP_GEARMAN_HOST "192.168.10.51"
#define DP_GEARMAN_PORT 7003

/* internal bool */
typedef _Bool dp_bool;
#define TRUE  (_Bool)1
#define FALSE (_Bool)0

/* empty string */
#define EMPTY_STRING ""

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
    int status;     /* exit status of child */
    dp_task task;   /* task associated with child */
    dp_bool null;   /* indicate empty entry */
    dp_bool update; /* indicate status update */
} dp_child;

/* task reply definition structure */
typedef struct dp_task_reply {
    char *bakctrace;
    char *error;
    char *status;
    char *result;
    char *message;
} dp_task_reply;

/* global child counter */
volatile int       child_counter = 0;         /* current number of running childern */
volatile dp_child  child_status[QUEUE_LIMIT]; /* array of child status */

/* internal functions */
dp_bool dp_signal_init    ();                  /* initialize signal handling (logged) */
dp_bool dp_signal_block   (sigset_t *old);     /* block SIGCHLD and return old mask */
dp_bool dp_signal_restore (sigset_t *restore); /* restore old mask */

dp_bool dp_gearman_init      (gearman_client_st **client);                            /* initialize gearman (logged) */
dp_bool dp_gearman_get_reply (dp_task_reply *reply, const char *result, size_t size); /* parse gearman reply */

dp_bool dp_mysql_init       (MYSQL **db);                       /* initialize MySQL (logged) */
dp_bool dp_mysql_connect    (MYSQL *db);                        /* connect to MySQL (logged) */
dp_bool dp_mysql_query      (MYSQL *db, const char *query);     /* execute MySQL query (logged), recover */
dp_bool dp_mysql_get_task   (dp_task *task, MYSQL_RES *result); /* extract MySQL stored task (logged) */
void    dp_mysql_task_free  (volatile dp_task *task);           /* free data associated with task */
void    dp_mysql_task_clear (volatile dp_task *task);           /* clear data associated with task */

void    dp_logger        (int priority, const char *message, ...); /* log message with specific priority */
int     dp_asprintf      (char **strp, const char *format, ...);   /* portability wrapper, allocated sprintf */
char   *dp_strcat        (const char *str, ...);                   /* concatenate string helper */
char   *dp_strdup        (const char *str);                        /* dup string helper */
char   *dp_strndup       (const char *str, size_t length);         /* sized dup string helper */
void    dp_sigchld       (int signal);                             /* SIGCHLD handler */
void    dp_status_update ();                                       /* process child_status table */

volatile dp_child *dp_child_null ();           /* find first null entry in child_status array */
volatile dp_child *dp_child_id   (int id);     /* find child with id in child_status array */
volatile dp_child *dp_child_pid  (pid_t pid);  /* find child with pid in child_status array */

int main()
{
    size_t pending_full_counter = 0;      /* count when queue is full and there are still pending tasks */
    size_t queue_counter = 0;             /* number of jobs in queue */
    size_t sense_counter = 0;             /* number of empty iterations */
    char query[QUERY_LIMIT], *aquery;     /* query buffer */
    gearman_client_st *client = NULL;
    MYSQL *db = NULL;
    sigset_t mask;

    /* initializa singal processing */
    if (!dp_signal_init())
        return EXIT_FAILURE;

    /* initialize temporary mask */
    sigprocmask(0, NULL, &mask);

    /* gearman initialize */
    if (!dp_gearman_init(&client))
        return EXIT_FAILURE;

    /* global MySQL init */
    my_init();

    /* initialize MySQL structure */
    if (!dp_mysql_init(&db)) {
        gearman_client_free(client);
        return EXIT_FAILURE;
    }

    /* connect to MySQL server */
    if (!dp_mysql_connect(db)) {
        gearman_client_free(client);
        mysql_close(db);
        return EXIT_FAILURE;
    }

    /* main loop */
    while (TRUE) {
        time_t timestamp;
        dp_task task;
        size_t rows = 0;
        pid_t pid;

        /* extract current number of child workers */
        queue_counter = child_counter;

        /* get timestamp */
        timestamp = time(NULL);

        /* START fake loop for query "exceptions" */
        do {
            MYSQL_RES *result;
            MYSQL_ROW row;

            /* prepare transaction */
            if (!dp_mysql_query(db, "BEGIN"))
                break;

            /* extract new works */
            snprintf(query, QUERY_LIMIT,
                     "SELECT * FROM deferred_tasks WHERE status = 'new' AND run_after < %ld ORDER BY priority DESC LIMIT 1 FOR UPDATE",
                     timestamp);
            if (!dp_mysql_query(db, query))
                break;

            /* process query result */
            result = mysql_store_result(db);
            if (result == NULL)
                rows = 0;
            else {
                rows = mysql_num_rows(result);
                if (rows >= 1 && queue_counter < QUEUE_LIMIT)
                    dp_mysql_get_task(&task, result);
                mysql_free_result(result);
            }

            /* update database */
            if (rows >= 1 && queue_counter < QUEUE_LIMIT) {
                snprintf(query, QUERY_LIMIT,
                         "UPDATE deferred_tasks SET status = 'working' WHERE id = %d", task.id);
                if (!dp_mysql_query(db, query))
                    break;
            }

            if (!dp_mysql_query(db, "COMMIT"))
                break;

        /* END fake loop for query "exceptions" */
        } while (FALSE);

        /* update basic counters */
        if (rows == 0) {
            sense_counter += 1;
            pending_full_counter = 0;
        } else
            sense_counter = 0;

        if (sense_counter >= SENSE_LIMIT) {
            dp_logger(LOG_NOTICE, "(%d/%d) ...", queue_counter, QUEUE_LIMIT);
            sense_counter = 0;
        }

        /* check if we can process job */
        if (rows >= 1 && queue_counter >= QUEUE_LIMIT)
            pending_full_counter += 1;

        /* check if we are overloaded */
        if (pending_full_counter >= PENDING_FULL_LIMIT) {
            dp_logger(LOG_WARNING, "KNOWN ISSUE NEX-1376: PPQ may be overloaded");
            pending_full_counter = 0;
        }

        if (rows >= 1 && queue_counter < QUEUE_LIMIT) {
            /* find first empty entry */
            volatile dp_child *worker = dp_child_null();

            /* initialize worker data */
            worker->task = task;
            worker->null = FALSE;

            /* block signals to ensure completness of worker structure */
            dp_signal_block(&mask);

            /* fork worker */
            if ((pid = fork()) == 0) { /* child */
                /* NOTE: children does not manage memory */

                gearman_return_t error;
                dp_task_reply reply;
                void *worker_result = NULL;
                size_t worker_result_size;

                dp_logger(LOG_DEBUG, "Forked worker (%d/%d) job (%d) -> %d",
                          queue_counter + 1, QUEUE_LIMIT, worker->task.id, getpid());

                worker_result = gearman_client_do(client,
                                                  worker->task.type,
                                                  NULL,
                                                  worker->task.description,
                                                  strlen(worker->task.description),
                                                  &worker_result_size,
                                                  &error);

                /* error executing work, retry */
                if (error) {
                    dp_logger(LOG_ERR, "Forked worker job (%d) FAILED (%d) -> %d",
                              worker->task.id, error, getpid());

                    /* prepare query */
                    snprintf(query, QUERY_LIMIT,
                             "UPDATE deferred_tasks SET status = 'new', result = '' WHERE id = %d",
                             worker->task.id);

                    /* execute query */
                    if (!dp_mysql_query(db, query))
                        return EXIT_FAILURE;
                    return error;
                }

                dp_gearman_get_reply(&reply, worker_result, worker_result_size);

                if (!strcmp(reply.status, ":ok"))
                    snprintf(query, QUERY_LIMIT,
                             "UPDATE deferred_tasks SET status = 'done', result = '%s' WHERE id = %d",
                             reply.result, worker->task.id);
                else {
                    dp_logger(LOG_ERR, "Forked worker job (%d) result in NOT ok (%s) -> %d",
                              worker->task.id, reply.status, getpid());

                    snprintf(query, QUERY_LIMIT,
                             "UPDATE deferred_tasks SET status = 'new', run_after = '%ld', result = '%s' WHERE id = %d",
                             timestamp + TIMESTAMP_DELAY, reply.result, worker->task.id);
                }

                /* execute query */
                if (!dp_mysql_query(db, query))
                    return EXIT_FAILURE;

                dp_logger(LOG_DEBUG, "Worker finished -> %d", getpid());
                return EXIT_SUCCESS;
            }

            /* detect fork error */
            /* fork failed */
            if (pid < 0) {
                /* TODO: update database */
                dp_logger(LOG_ERR, "Fork failed!");
            /* fork successfull */
            } else {
                /* update worker status */
                worker->pid = pid;
                /* increase task count */
                child_counter += 1;
            }

            /* restore signals */
            dp_signal_restore(&mask);
        }

        /* wait for next iteration */
        /* NOTE: sleep is broken when we receive signal */
        /* NOTE: sleep only when no task are waiting or we have full queue */

        if (rows == 0 ||
            (rows >= 1 && queue_counter >= QUEUE_LIMIT))
            for (uint timeout = SLEEP_TIMEOUT; timeout > 0;)
                timeout = sleep(timeout);

        /* update status of workers */
        dp_status_update();
    }

    gearman_client_free(client);
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
        child_status[i].status = 0;
        child_status[i].null = TRUE;
        child_status[i].update = FALSE;
    }

    /* setup singals */
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
                  gearman_client_error(*client));
        return FALSE;
    }

    dp_logger(LOG_INFO, "Gearman connection established");
    return TRUE;
}

dp_bool dp_gearman_get_reply(dp_task_reply *reply, const char *result, size_t size)
{
    char *str, *end;

    /* initialize result */
    reply->result = EMPTY_STRING;
    reply->error = EMPTY_STRING;
    reply->bakctrace = EMPTY_STRING;
    reply->message = EMPTY_STRING;
    reply->status = EMPTY_STRING;

    /* quick hack to get status */
    if ((str = strstr(result, ":status:")) == NULL)
        return FALSE;
    str += 8;

    if ((end = strchr(str, '\n')) == NULL)
        end = str + strlen(str);

    reply->result = dp_strndup(str, end - str);

    /* TODO: escape string for MySQL */

    return TRUE;
}

/* basic, logged initialization of MySQL */
dp_bool dp_mysql_init(MYSQL **db)
{
    if ((*db = mysql_init(NULL)) == NULL) {
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
        dp_logger(LOG_ERR, "MySQL query error (%d), recovering: %s",
                  mysql_errno(db),
                  mysql_error(db));

        switch (mysql_errno(db)) {
            case CR_COMMANDS_OUT_OF_SYNC:  /* try to recover */
                result = mysql_store_result(db);
                mysql_free_result(result);
                return FALSE;
            case CR_SERVER_GONE_ERROR:     /* try to reconnect */
            case CR_SERVER_LOST:
                dp_mysql_connect(db);
                return FALSE;
            default:
                return FALSE;
        }
    }

    return TRUE;
}

/* convert single row into typed task */
/* TODO: error chedp_mysql_task_clearcking for mysql_fetch_row */
dp_bool dp_mysql_get_task(dp_task *task, MYSQL_RES *result)
{
    char buffer[BUFFER_LIMIT];
    MYSQL_FIELD *field;
    MYSQL_ROW row;
    size_t size;

    /* init task structure */
    memset(task, 0, sizeof(dp_task));

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

void dp_mysql_task_free(volatile dp_task *task)
{
    if (task != NULL) {
        free(task->type);
        free(task->description);
        free(task->status);
    }
}

void dp_mysql_task_clear(volatile dp_task *task)
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

char *dp_strndup(const char *str, size_t length)
{
    char *dup;

    if ((dup = malloc((length + 1))) == NULL)
        return NULL;

    /* copy string, only size! */
    memcpy(dup, str, length);
    dup[length] = '\0';

    return dup;
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

    /* finnalize string */
    *cat = '\0';

    /* return string */
    return cat - size;
}

void dp_logger(int priority, const char *message, ...)
{
    va_list args;

    /* ensure to use daeamon facility */
    priority &= LOG_FACMASK;
    priority |= LOG_DAEMON;

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
    volatile dp_child *worker = NULL;
    int status;
    pid_t pid;

    /* wait for first child */
    pid = wait(&status);

    if (pid < 0)
        return;

    /* update status */
    if (worker = dp_child_pid(pid)) {
        worker->status = status;
        worker->update = TRUE;
    }
}

void dp_status_update()
{
    for (size_t i = 0; i < QUEUE_LIMIT; ++i) {
        dp_bool is_end = FALSE;
        sigset_t mask;

        /* check if entry contain child and is updated */
        if (child_status[i].null || !child_status[i].update)
            continue;

        /* critical section, block signals */
        dp_signal_block(&mask);

        /* cache worker pointer */
        volatile dp_child *worker = &child_status[i];

        /* check if normal exit */
        if (WIFEXITED(worker->status)) {
            child_counter -= 1;
            is_end = TRUE;

            if (WEXITSTATUS(worker->status) != EXIT_SUCCESS)
                dp_logger(LOG_ERR,
                          "Child worker (%d) invalid exit (%d)",
                          worker->pid, WEXITSTATUS(worker->status));

        /* check type of signal */
        } else if (WIFSIGNALED(worker->status)) {
            if (WTERMSIG(worker->status) || WCOREDUMP(worker->status)) {
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

        /* mark as updated */
        worker->update = FALSE;

        if (is_end) {
            /* clear task data */
            dp_mysql_task_free(&worker->task);
            dp_mysql_task_clear(&worker->task);

            /* clear worker data */
            worker->pid = 0;
            worker->status = 0;
            worker->null = TRUE;
        }

        /* critical section ends, unblock signals */
        dp_signal_restore(&mask);
    }
}

volatile dp_child *dp_child_null()
{
    for (size_t i = 0; i < QUEUE_LIMIT; ++i)
        if (child_status[i].null)
            return &child_status[i];
}

volatile dp_child *dp_child_task_id(int id)
{
    for (size_t i = 0; i < QUEUE_LIMIT; ++i)
        if (!child_status[i].null)
            if (child_status[i].task.id == id)
                return &child_status[i];
}

volatile dp_child *dp_child_pid(pid_t pid)
{
    for (size_t i = 0; i < QUEUE_LIMIT; ++i)
        if (!child_status[i].null)
            if (child_status[i].pid == pid)
                return &child_status[i];
}

