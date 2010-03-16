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
#include <mysql/mysqld_error.h> /* mysql errno's */
#include <libgearman/gearman.h> /* gearman */
#include <yaml.h>               /* yaml */

/* check macros */
#ifdef __GNUC__
#define ATTRIBUTE_PRINTF(A1,A2) __attribute__ ((format (printf,A1,A2)))
#define ATTRIBUTE_SENTINEL      __attribute__ ((sentinel))
#define ATTRIBUTE_NONNULL(...)  __attribute__ ((nonnull (__VA_ARGS__)))
#else
#define ATTRIBUTE_PRINTF(A1,A2)
#define ATTRIBUTE_SENTINEL
#define ATTRIBUTE_NONNULL(...)
#endif /* __GNUC__ */

/* global defines */
#define BUFFER_QUERY          8192  /* initial and minimal size of query buffer */
#define BUFFER_YAML           4096  /* initial and minimal size of yaml buffer */
#define BUFFER_SIZE_MAX       1024  /* length of internal buffers */
#define FORCE_TERMINATE_COUNT 3     /* number of terminate signals that force quit */

#define TIMESPEC_0_1_SEC      {0, 100000000L}  /* 0.1 sec as struct timespec */
#define NSEC_IN_SEC           1000000000L      /* number of nsec in 1 sec */

/* internal errors for MySQL results */
/* NOTE: proper escaping for MySQL */
#define RESULT_ERROR_GEARMAN  "---\\n:status: :fail\\n:message: gearman work not successful\\n"
#define RESULT_ERROR_YAML     "---\\n:status: :fail\\n:message: yaml parsing error\\n"

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
        uint16_t failed_delay;
        uint16_t timeout_delay;
        char *environment;
        bool priority;
    } task;

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

/* simple buffer */
typedef struct dp_buffer {
    char *str;        /* string associated with buffer */
    size_t size;      /* length of the buffer string */
    size_t pool;      /* allocated length of buffer */
} dp_buffer;

/* task definition structure */
typedef struct dp_task {
    int id;            /* id number of the task */
    int priority;      /* priority of the task */
    char *type;        /* call destination for gearman */
    char *description; /* call parameters for gearman */
    char *status;      /* task status, i.e. new, working, done */
    time_t run_after;  /* timestamp for delayed execution, timeouts handling */
} dp_task;

/* task result definition structure */
typedef struct dp_task_result {
    bool status;
    double time_elapsed;
} dp_task_result;

/* child definition structure */
typedef struct dp_child {
    pid_t pid;      /* pid of child */
    dp_task task;   /* task associated with child */
    time_t stamp;   /* stamp associated with child */
    bool null;      /* indicate empty entry */
} dp_child;

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
    DP_CONFIG_TASK_FAILED_DELAY,
    DP_CONFIG_TASK_TIMEOUT_DELAY,
    DP_CONFIG_TASK_ENVIRONMENT,
    DP_CONFIG_TASK_PRIORITY,
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

dp_enum dp_config_value[] = {
    {"mysql_host", DP_CONFIG_MYSQL_HOST},
    {"mysql_db", DP_CONFIG_MYSQL_DB},
    {"mysql_user", DP_CONFIG_MYSQL_USER},
    {"mysql_passwd", DP_CONFIG_MYSQL_PASSWD},
    {"mysql_table", DP_CONFIG_MYSQL_TABLE},
    {"mysql_port", DP_CONFIG_MYSQL_PORT},
    {"gearman_host", DP_CONFIG_GEARMAN_HOST},
    {"gearman_port", DP_CONFIG_GEARMAN_PORT},
    {"task_failed_delay", DP_CONFIG_TASK_FAILED_DELAY},
    {"task_timeout_delay", DP_CONFIG_TASK_TIMEOUT_DELAY},
    {"task_environment", DP_CONFIG_TASK_ENVIRONMENT},
    {"task_priority", DP_CONFIG_TASK_PRIORITY},
    {"log_dispatcher", DP_CONFIG_LOG_DISPATCHER},
    {"log_worker", DP_CONFIG_LOG_WORKER},
    {"log_level", DP_CONFIG_LOG_LEVEL},
    {"log_facility", DP_CONFIG_LOG_FACILITY},
    {"sense_loop", DP_CONFIG_SENSE_LOOP},
    {"sense_terminated", DP_CONFIG_SENSE_TERMINATED},
    {"sense_paused", DP_CONFIG_SENSE_PAUSED},
    {"sleep_loop", DP_CONFIG_SLEEP_LOOP},
    {NULL, 0}
};

/* global flag to indicate child state change */
volatile sig_atomic_t child_flag = 0;

/* global flag to pause dispatching */
volatile sig_atomic_t pause_flag = 0;

/* global flag to terminate dispatcher */
volatile sig_atomic_t terminate_flag = 0;

/* global flag to reload configuration */
volatile sig_atomic_t reload_flag = 0;

/* global flag to output status */
volatile sig_atomic_t status_flag = 0;

/* global status variables */
int16_t   child_counter = 0;        /* current number of running children */
dp_child *child_status = NULL;      /* array of child status */
uint8_t   child_limit = 50;         /* maximum number of children */

/* global configuration */
const char *cfg_location = NULL;    /* configuration file location override */
dp_config cfg;                      /* global configuration object */

/* global state flags */
bool initialized = false;           /* flag to indicate basic initialization */

/*
 * fork helper functions
 */

/* forked child function */
int dp_fork_exec(dp_child *worker);

/* initialize signal handling (logged) for fork */
bool dp_fork_signal_init();

/*
 * signal management functions
 */

/* initialize signal handling (logged) */
bool dp_signal_init();

/* block specific signal */
bool dp_signal_block(int signum);

/* unblock specific signal */
bool dp_signal_unblock(int signum);

/*
 * dispatcher config functions
 */

/* initialize configuration */
bool dp_config_init();

/* get config field id */
dp_config_val dp_config_field(const char *name);

/* assign field value in config */
bool dp_config_set(dp_config *config,
                   dp_config_val field,
                   char *value,
                   bool if_dup);

/* free data associated with config */
void dp_config_free(dp_config *config);

/*
 * string buffer functions
 */

/* allocate buffer with specific pool size */
dp_buffer *dp_buffer_new(size_t pool);

/* initialize buffer with specific pool size */
dp_buffer *dp_buffer_init(dp_buffer *buf,
                          size_t pool);

/* free buffer */
void dp_buffer_free(dp_buffer *buf);

/* insert format string, grow as necessary */
dp_buffer *dp_buffer_printf(dp_buffer *buf,
                            const char *format, ...) ATTRIBUTE_PRINTF(2, 3);

/* append buffer to buffer, grow as necessary */
dp_buffer *dp_buffer_append(dp_buffer *buf,
                            const dp_buffer *append);

/*
 * gearman helper functions */

/* initialize gearman (logged) */
bool dp_gearman_init(gearman_client_st **client);

/* get status from gearman reply */
bool dp_gearman_get_result(dp_task_result *result,
                           const char *worker_result,
                           size_t worker_result_size);

/*
 * MySQL wrapper functions
 */

/* initialize MySQL (logged) */
bool dp_mysql_init(MYSQL **db);

/* connect to MySQL (logged) */
bool dp_mysql_connect(MYSQL *db);

/* execute MySQL query (logged), recover connection and retry if possible */
bool dp_mysql_query(MYSQL *db,
                    const char *query,
                    bool if_retry);

/* extract MySQL stored task (logged) */
bool dp_mysql_get_task(dp_task *task,
                       MYSQL_RES *result);

/* extract MySQL int variable (logged) */
bool dp_mysql_get_int(int *value,
                      MYSQL_RES *result);

/* free data associated with task */
void  dp_mysql_task_free(dp_task *task);

/* clear data associated with task */
void  dp_mysql_task_clear(dp_task *task);

/*
 * YAML parsing functions
 */

/* write handler for yaml emitters */
int dp_yaml_write_handler(void *data,
                          unsigned char *buffer,
                          size_t size);

/* process task description */
dp_buffer *dp_yaml_task_description(const dp_task *task);

/* add map node at document root */
bool dp_yaml_add_map_node(yaml_document_t *document,
                          const char *key,
                          const char *value);

/* add int map node at document root */
bool dp_yaml_add_map_node_int(yaml_document_t *document,
                              const char *key,
                              int value);

/* get field value (single-line) from string */
char *dp_yaml_field_line(const char *yaml,
                         const char *field) ATTRIBUTE_NONNULL(1, 2);

/*
 * logging functions
 */

/* initialize logging capabilities */
void  dp_logger_init(const char *ident);

/* log message with specific priority */
void  dp_logger(int priority,
                const char *message, ...) ATTRIBUTE_PRINTF(2, 3);

/*
 * enum helpers
 */

/* extract specific enum by name */
dp_enum *dp_enum_name(dp_enum *self,
                      const char *name);

/* extract specific enum by value */
dp_enum *dp_enum_value(dp_enum *self,
                       int value);

/*
 * basic string helpers
 */

/* dup string helper */
char *dp_strdup(const char *str);

/* sized dup string helper */
char *dp_strudup(const char *str,
                 size_t length);

/* sized strchr string helper */
char *dp_struchr(const char *str,
                 size_t length,
                 char character);

/* sized strstr string helper */
char *dp_strustr(const char *str,
                 size_t length,
                 const char *locate);

/* concatenate string helper */
char *dp_strcat(const char *str, ...) ATTRIBUTE_SENTINEL;

/* escape string helper */
char *dp_strescape(const char *str);

/* sized escape string helper */
char *dp_struescape(const char *str,
                    size_t length);

/*
 * timespec operations
 */

/* multiply timespec be multiplier */
void   dp_timespec_mul(struct timespec *self,
                       double multiplier);

/* check if timespec is more that value in sec */
bool   dp_timespec_more(struct timespec *self,
                        double value);

/* return floating point representation of timespec in secs */
double dp_timespec_double(struct timespec *self);

/*
 * signal handlers
 */

void  dp_sigchld(int signal);
void  dp_sighup(int signal);
void  dp_sigtermint(int signal);
void  dp_sigusr1(int signal);
void  dp_sigusr2(int signal);

/*
 * dispatcher status processing
 */

/* initialize child_status table */
bool  dp_status_init();

/* free data associated with child_status table */
void  dp_status_free();

/* process child_status table */
void  dp_status_update();

/* process child_status table timeouts */
void  dp_status_timeout(time_t timestamp);

/*
 * dispatcher status search
 */

/* find first null entry in child_status array */
dp_child *dp_child_null();

/* find child with pid in child_status array */
dp_child *dp_child_pid(pid_t pid);

