#include "config.h"

/* standard headers */
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdbool.h>  /* _Bool */
#include <stdint.h>   /* int32_t */
#include <time.h>     /* time */
#include <ctype.h>    /* isspace */

/* other headers */
#include <unistd.h>             /* sleep, fork */
#include <signal.h>             /* SIGCHLD handling */
#include <sys/wait.h>           /* wait */
#include <syslog.h>             /* system log */
#include <mysql/mysql.h>        /* mysql */
#include <mysql/errmsg.h>
#include <libgearman/gearman.h> /* gearman */

/* check macros */
#ifdef __GNUC__
#define ATTRIBUTE_PRINTF(A1,A2) __attribute__ ((format  (printf,A1,A2)))
#define ATTRIBUTE_SENTINEL      __attribute__ ((sentinel))
#define ATTRIBUTE_NONNULL(...)  __attribute__ ((nonnull (__VA_ARGS__)));
#else
#define ATTRIBUTE_PRINTF(A1,A2)
#define ATTRIBUTE_SENTINEL
#define ATTRIBUTE_NONNULL(...)
#endif /* __GNUC__ */

/* global defines */
#define QUEUE_LIMIT        50    /* maximum number of concurrent workers */
#define QUERY_LIMIT        8192  /* maximum MySQL query length */
#define BUFFER_LIMIT       1024  /* maximal length of internal buffers */
#define SENSE_LIMIT        30    /* sense log delay */
#define TERMINATE_LIMIT    5     /* sense when terminated log delay */
#define PAUSE_LIMIT        15    /* sense when paused log delay */
#define SLEEP_TIMEOUT      1     /* main loop timeout */
#define TIMESTAMP_DELAY    5*60  /* task execution delay */

/* syslog identifiers */
#define DP_PARENT       "dispatcher"
#define DP_CHILD        "worker"

/* mysql server info */
#define DP_MYSQL_HOST   "devdba"
#define DP_MYSQL_USER   "root"
#define DP_MYSQL_PASSWD "Ohio"
#define DP_MYSQL_DB     "processqueue_dev"
#define DP_MYSQL_PORT   0
#define DP_MYSQL_TABLE  "deferred_tasks_new"

/* gearman server info */
#define DP_GEARMAN_HOST "dev"
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
typedef struct dp_reply {
    char *backtrace;
    char *error;
    char *status;
    char *result;
    char *message;
} dp_reply;

/* task reply field ids */
typedef enum dp_reply_val {
    DP_REPLY_UNKNOWN,
    DP_REPLY_BACKTRACE,
    DP_REPLY_ERROR,
    DP_REPLY_STATUS,
    DP_REPLY_RESULT,
    DP_REPLY_MESSAGE,
} dp_reply_val;

/* global flag to indicate child state change */
volatile sig_atomic_t child_flag = FALSE;

/* global flag to pause dispatching */
volatile sig_atomic_t pause_flag = FALSE;

/* global flag to terminate dispatcher */
volatile sig_atomic_t terminate_flag = FALSE;

/* global status variables */
int      child_counter = 0;         /* current number of running children */
dp_child child_status[QUEUE_LIMIT]; /* array of child status */

/* internal functions */
dp_bool dp_signal_init    ();                  /* initialize signal handling (logged) */
dp_bool dp_signal_block   (sigset_t *old);     /* block SIGCHLD and return old mask */
dp_bool dp_signal_restore (sigset_t *restore); /* restore old mask */

dp_bool dp_gearman_init         (gearman_client_st **client);                       /* initialize gearman (logged) */
dp_bool dp_gearman_get_reply    (dp_reply *reply, const char *result, size_t size); /* parse gearman reply */

dp_reply_val dp_gearman_reply_field  (const char *name);                                       /* get task reply field id from name */
const char  *dp_gearman_reply_value  (dp_reply *reply, dp_reply_val field);                    /* get value of reply field */
dp_bool      dp_gearman_reply_set    (dp_reply *reply, dp_reply_val field, char *value);       /* assign name value to reply */
dp_bool      dp_gearman_reply_escape (dp_reply *reply, dp_reply_val field);                    /* escape name value in reply */
void         dp_gearman_reply_free   (dp_reply *reply);                                        /* free data associated with reply */

dp_bool dp_mysql_init       (MYSQL **db);                              /* initialize MySQL (logged) */
dp_bool dp_mysql_connect    (MYSQL *db);                               /* connect to MySQL (logged) */
dp_bool dp_mysql_query      (MYSQL *db, const char *query);            /* execute MySQL query (logged), recover */
dp_bool dp_mysql_get_task   (dp_task *task, MYSQL_RES *result);        /* extract MySQL stored task (logged) */
dp_bool dp_mysql_get_int    (int *value, MYSQL_RES *result);           /* extract MySQL int variable (logged) */

void    dp_mysql_task_free  (dp_task *task);                           /* free data associated with task */
void    dp_mysql_task_clear (dp_task *task);                           /* clear data associated with task */

void    dp_logger_init   (const char *ident);                                  /* initialize logging capabilities */
void    dp_logger        (int priority, const char *message, ...)              /* log message with specific priority */
                          ATTRIBUTE_PRINTF(2,3);
int     dp_asprintf      (char **strp, const char *format, ...)                /* portability wrapper, allocated sprintf */
                          ATTRIBUTE_PRINTF(2,3);
char   *dp_strdup        (const char *str);                                    /* dup string helper */
char   *dp_strudup       (const char *str, size_t length);                     /* sized dup string helper */
char   *dp_struchr       (const char *str, size_t length, char character);     /* sized strchr string helper */
char   *dp_strustr       (const char *str, size_t length, const char *locate); /* sized strstr string helper */
char   *dp_strcat        (const char *str, ...)                                /* concatenate string helper */
                          ATTRIBUTE_SENTINEL;
void    dp_sigchld       (int signal);                                         /* SIGCHLD handler */
void    dp_sighup        (int signal);                                         /* SIGHUP handler */
void    dp_sigterm       (int signal);                                         /* SIGTERM handler */
dp_bool dp_status_init   ();                                                   /* initialize child_status table */
void    dp_status_update (int32_t *queue_counter);                             /* process child_status table */

dp_child *dp_child_null ();           /* find first null entry in child_status array */
dp_child *dp_child_pid  (pid_t pid);  /* find child with pid in child_status array */
