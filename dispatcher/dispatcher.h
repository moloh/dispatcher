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
#include <unistd.h>             /* sleep, fork, getopt */
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

/* internal errors for MySQL results */
/* NOTE: proper escaping for MySQL */
#define ERROR_ASPRINTF "---\\n:status: :asprintf\\n"
#define ERROR_FORK     "---\\n:status: :fork\\n"

/* internal bool */
#define TRUE  true
#define FALSE false

#if 0
#undef LOG_WARNING
#undef LOG_DEBUG
#undef LOG_INFO
#define LOG_WARNING LOG_ERR
#define LOG_DEBUG LOG_ERR
#define LOG_INFO LOG_ERR
#endif

/* dispatcher configuration */
typedef struct dp_config {
    struct {
        char *host;
        char *db;
        char *user;
        char *passwd;
        char *table;
        int port;
    } mysql;

    struct {
        char *host;
        int port;
    } gearman;

    struct {
        uint16_t task_failed;
        uint16_t task_timeout;
    } delay;

    struct {
        char *dispatcher;
        char *worker;
        int level;
        int facility;
    } log;

    struct {
        uint16_t loop;
        uint16_t terminated;
        uint16_t paused;
    } sense;

    uint16_t sleep_loop;
} dp_config;

typedef struct dp_enum {
    const char *name;
    int value;
} dp_enum;

/* task definition structure */
typedef struct dp_task {
    int id;            /* id number of the task */
    int priority;      /* priority of the task */
    char *type;        /* function name for gearman */
    char *description; /* function parameters for gearman */
    char *status;      /* task status, i.e. new, working, done */
    time_t run_after;  /* timestamp for delayed execution, timeouts handling */
} dp_task;

/* child definition structure */
typedef struct dp_child {
    pid_t pid;      /* pid of child */
    dp_task task;   /* task associated with child */
    time_t stamp;   /* stamp associated with child */
    bool null;   /* indicate empty entry */
} dp_child;

/* task reply definition structure */
typedef struct dp_reply {
    char *backtrace; /* backtrace field from task reply */
    char *error;     /* error field from task reply */
    char *status;    /* status field from task reply */
    char *result;    /* result field from task reply */
    char *message;   /* message field from task reply */
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

/* configuration fields ids */
typedef enum dp_config_val {
    DP_CONFIG_UNKNOWN,
    DP_CONFIG_MYSQL_HOST,
    DP_CONFIG_MYSQL_DB,
    DP_CONFIG_MYSQL_USER,
    DP_CONFIG_MYSQL_PASSWD,
    DP_CONFIG_MYSQL_TABLE,
    DP_CONFIG_MYSQL_PORT,
    DP_CONFIG_GEARMAN_HOST,
    DP_CONFIG_GEARMAN_PORT,
    DP_CONFIG_DELAY_TASK_FAILED,
    DP_CONFIG_DELAY_TASK_TIMEOUT,
    DP_CONFIG_LOG_DISPATCHER,
    DP_CONFIG_LOG_WORKER,
    DP_CONFIG_LOG_LEVEL,
    DP_CONFIG_LOG_FACILITY,
    DP_CONFIG_SENSE_LOOP,
    DP_CONFIG_SENSE_TERMINATED,
    DP_CONFIG_SENSE_PAUSED,
    DP_CONFIG_SLEEP_LOOP
} dp_config_val;

/* define supported facilites in configuration file */
dp_enum dp_log_facility[] = {
    {"LOG_USER", LOG_USER},
    {"LOG_DAEMON", LOG_DAEMON},
    {"LOG_LOCAL0", LOG_LOCAL0},
    {"LOG_LOCAL1", LOG_LOCAL1},
    {"LOG_LOCAL2", LOG_LOCAL2},
    {"LOG_LOCAL3", LOG_LOCAL3},
    {"LOG_LOCAL4", LOG_LOCAL4},
    {"LOG_LOCAL5", LOG_LOCAL5},
    {"LOG_LOCAL6", LOG_LOCAL6},
    {"LOG_LOCAL7", LOG_LOCAL7},
    {NULL, 0}
};

/* define supported levels in configuration file */
dp_enum dp_log_level[] = {
    {"LOG_EMERG", LOG_EMERG},
    {"LOG_ALERT", LOG_ALERT},
    {"LOG_CRIT", LOG_CRIT},
    {"LOG_ERR", LOG_ERR},
    {"LOG_WARNING", LOG_WARNING},
    {"LOG_NOTICE", LOG_NOTICE},
    {"LOG_INFO", LOG_INFO},
    {"LOG_DEBUG", LOG_DEBUG},
    {NULL, 0}
};

/* global flag to indicate child state change */
volatile sig_atomic_t child_flag = FALSE;

/* global flag to pause dispatching */
volatile sig_atomic_t pause_flag = FALSE;

/* global flag to terminate dispatcher */
volatile sig_atomic_t terminate_flag = FALSE;

/* global flag to reload configuration */
volatile sig_atomic_t reload_flag = FALSE;

/* global status variables */
int      child_counter = 0;         /* current number of running children */
dp_child child_status[QUEUE_LIMIT]; /* array of child status */

/* global configuration */
const char *cfg_location = NULL;    /* configuration file location override */
dp_config cfg;                      /* global configuration object */

/* global state flags */
bool initialized = FALSE;           /* flag to indicate basic initialization */

/* internal functions */
bool dp_config_init    ();                  /* initialize configuration */
bool dp_signal_init    ();                  /* initialize signal handling (logged) */
bool dp_signal_block   (sigset_t *old);     /* block SIGCHLD and return old mask */
bool dp_signal_restore (sigset_t *restore); /* restore old mask */

dp_config_val dp_config_field (const char *name);                                                 /* get config field id */
bool          dp_config_set   (dp_config *config, dp_config_val field, char *value, bool if_dup); /* assign field value in config */
void          dp_config_free  (dp_config *config);                                                /* free data associated with config */

bool dp_gearman_init         (gearman_client_st **client);                       /* initialize gearman (logged) */
bool dp_gearman_get_reply    (dp_reply *reply, const char *result, size_t size); /* parse gearman reply */
bool dp_gearman_get_status   (const char *result, size_t size);                  /* get status from gearman reply */

dp_reply_val dp_gearman_reply_field  (const char *name);                                 /* get task reply field id from name */
const char  *dp_gearman_reply_value  (dp_reply *reply, dp_reply_val field);              /* get value of reply field */
bool         dp_gearman_reply_set    (dp_reply *reply, dp_reply_val field, char *value); /* assign field value in reply */
bool         dp_gearman_reply_escape (dp_reply *reply, dp_reply_val field);              /* escape field value in reply */
void         dp_gearman_reply_free   (dp_reply *reply);                                  /* free data associated with reply */

bool  dp_mysql_init       (MYSQL **db);                              /* initialize MySQL (logged) */
bool  dp_mysql_connect    (MYSQL *db);                               /* connect to MySQL (logged) */
bool  dp_mysql_query      (MYSQL *db, const char *query);            /* execute MySQL query (logged), recover */
bool  dp_mysql_get_task   (dp_task *task, MYSQL_RES *result);        /* extract MySQL stored task (logged) */
bool  dp_mysql_get_int    (int *value, MYSQL_RES *result);           /* extract MySQL int variable (logged) */

void  dp_mysql_task_free  (dp_task *task);                           /* free data associated with task */
void  dp_mysql_task_clear (dp_task *task);                           /* clear data associated with task */

void  dp_logger_init   (const char *ident);                                  /* initialize logging capabilities */
void  dp_logger        (int priority, const char *message, ...)              /* log message with specific priority */
                        ATTRIBUTE_PRINTF(2,3);

dp_enum *dp_enum_name (dp_enum *self, const char *name);                     /* extract specific enum by name */
dp_enum *dp_enum_value(dp_enum *self, int value);                            /* extract specific enum by value */

int   dp_asprintf      (char **strp, const char *format, ...)                /* portability wrapper, allocated sprintf */
                        ATTRIBUTE_PRINTF(2,3);
char *dp_strdup        (const char *str);                                    /* dup string helper */
char *dp_strudup       (const char *str, size_t length);                     /* sized dup string helper */
char *dp_struchr       (const char *str, size_t length, char character);     /* sized strchr string helper */
char *dp_strustr       (const char *str, size_t length, const char *locate); /* sized strstr string helper */
char *dp_strcat        (const char *str, ...)                                /* concatenate string helper */
                        ATTRIBUTE_SENTINEL;
char *dp_strescape     (const char *str);                                    /* escape string helper */
char *dp_struescape    (const char *str, size_t length);                     /* sized escape string helper */

void  dp_sigchld       (int signal);                                         /* SIGCHLD handler */
void  dp_sighup        (int signal);                                         /* SIGHUP handler */
void  dp_sigtermint    (int signal);                                         /* SIGTERM handler */
void  dp_sigusr12      (int signal);                                         /* SIGUSR12 handler */

bool  dp_status_init   ();                                                   /* initialize child_status table */
void  dp_status_update (int32_t *queue_counter);                             /* process child_status table */
void  dp_status_timeout(time_t timestamp, int32_t *queue_counter);           /* process child_status table timeouts */

dp_child *dp_child_null ();           /* find first null entry in child_status array */
dp_child *dp_child_pid  (pid_t pid);  /* find child with pid in child_status array */

