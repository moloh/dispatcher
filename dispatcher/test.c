#include "dispatcher.h"

#define DP_RUN_TEST(name) do {                                                 \
    printf("Run "#name"\n");                                                   \
    if (!name()) {                                                             \
        printf("Run "#name" - failed\n");                                      \
        dp_test_all += 1;                                                      \
        dp_test_fail += 1;                                                     \
    } else {                                                                   \
        printf("Run "#name" - success\n");                                     \
        dp_test_all += 1;                                                      \
    }                                                                          \
} while(0)

bool dp_buffer_printf_test();
bool dp_buffer_append_printf_test();

int main()
{
    int dp_test_all = 0;
    int dp_test_fail = 0;

    DP_RUN_TEST(dp_buffer_printf_test);
    DP_RUN_TEST(dp_buffer_append_printf_test);

    printf("%d tests, %d failures\n",
           dp_test_all,
           dp_test_fail);

    if (dp_test_fail)
        return EXIT_FAILURE;
    return EXIT_SUCCESS;
}

bool dp_buffer_printf_test()
{
    dp_buffer *buf = dp_buffer_new(0);
    if (dp_buffer_printf(buf, "%d int, %s string", 2, "foo") == NULL)
        return false;

    if(strcmp(buf->str, "2 int, foo string"))
        return false;

    dp_buffer_free(buf);
    return true;
}

bool dp_buffer_append_printf_test()
{
    return true;
}

