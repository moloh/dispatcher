#!/bin/sh

run_auto() {
    echo "Running $1 ..."
    $@

    if [ $? -ne 0 ]; then
        echo "Operation failed!" 
        exit
    fi
}

run_auto aclocal
run_auto autoheader
run_auto autoconf
run_auto ./configure $@

echo "Build system regenerated successfully"
