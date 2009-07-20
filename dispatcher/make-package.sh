#!/bin/bash -e

# lets update configure and config.h.in
autoheader
autoconf

RELEASE="$(sed -n "s/^PACKAGE_VERSION='\(.*\)'$/\1/p" configure)"
REVISION="$(svn info . | grep '^Revision: ' | sed -e 's/[^0-9]//g')"
PACKAGE="nexopia-dispatcher-${RELEASE}-r${REVISION}"

echo "Building source package '${PACKAGE}.tar.gz'."
svn export . "${PACKAGE}"
tar cvfz "${PACKAGE}.tar.gz" "${PACKAGE}"
rm -rfv "${PACKAGE}"
echo "Completed build of source package '${PACKAGE}.tar.gz'."
