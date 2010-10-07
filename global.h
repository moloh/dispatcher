#ifndef __DISPATCHER_GLOBAL_H__
#define __DISPATCHER_GLOBAL_H__

#include "dispatcher.h"

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

/* global buffers */
dp_buffer *query = NULL;            /* query buffer */

#endif /* !__DISPATCHER_GLOBAL_H__ */

