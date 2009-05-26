# we have to use gnu gnu99 (not c99)
# most headers are not compatible

# basic variables, hack rpath for /nexopia runtime
PREFIX = /nexopia
CC = $(shell which gcc)
INSTALL = $(shell which install)
CFLAGS = -std=gnu99 -g
LDFLAGS = -Wl,-rpath=$(PREFIX)/lib

# we use thread safe version of library
MYSQL_CFLAGS = -I$(PREFIX)/include
MYSQL_LDFLAGS = -L$(PREFIX)/lib/mysql -lmysqlclient_r -Wl,-rpath=$(PREFIX)/lib/mysql -Wl,-O1

# hack for local access
GEARMAN_CFLAGS = $(shell PKG_CONFIG_PATH=$(PREFIX)/lib/pkgconfig pkg-config gearmand --cflags)
GEARMAN_LDFLAGS = $(shell PKG_CONFIG_PATH=$(PREFIX)/lib/pkgconfig pkg-config gearmand --libs)

SOURCE = dispatcher.c
OBJECT = dispatcher.o

# setup final compiler flags
CFLAGS_REAL = $(DEFINES) $(CFLAGS) $(MYSQL_CFLAGS) $(GEARMAN_CFLAGS)
LDFLAGS_REAL = $(LDFLAGS) $(MYSQL_LDFLAGS) $(GEARMAN_LDFLAGS)

.c.o:
	$(CC) $(CFLAGS_REAL) -c $*.c -o $*.o

dispatcher: $(OBJECT)
	$(CC) $(OBJECT) $(LDFLAGS_REAL) -o dispatcher

clean:
	rm $(OBJECT)
	rm dispatcher

all: dispatcher

install:
	$(INSTALL) -m 755 dispatcher $(PREFIX)/bin

.PHONY: all clean install
