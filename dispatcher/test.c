#include "dispatcher.h"

#define DP_
#define DP_RUN_TEST(name) do {         \
    printf("Running "#name);           \
    dp_test_all += 1;                  \
    if (!name()) {                     \
        printf(" - failed\n");         \
        dp_test_fail += 1;             \
    }                                  \
} while(0)

int main()
{
    int dp_test_all = 0;
    int dp_test_fail = 0;

    printf("%d tests, %d failures\n",
           dp_test_all,
           dp_test_fail);

    if (dp_test_fail)
        return EXIT_FAILURE;
    return EXIT_SUCCESS;
}

