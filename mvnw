#!/bin/sh

# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Apache Maven Wrapper startup batch script, version @@project.version@@
#
# Required ENV vars:
# ------------------
#   JAVA_HOME - location of a JDK home dir
#
# Optional ENV vars
# -----------------
#   M2_HOME - location of maven2's installed home dir
#   MAVEN_OPTS - parameters passed to the Java VM when running Maven
#     e.g. to debug Maven itself, use
#       set MAVEN_OPTS=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000
#   MAVEN_SKIP_RC - flag to disable loading of mavenrc files
# ----------------------------------------------------------------------------

if [ -z "$MAVEN_SKIP_RC" ] ; then

  if [ -f /usr/local/etc/mavenrc ] ; then
    . /usr/local/etc/mavenrc
  fi

  if [ -f /etc/mavenrc ] ; then
    . /etc/mavenrc
  fi

  if [ -f "$HOME/.mavenrc" ] ; then
    . "$HOME/.mavenrc"
  fi

fi

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
mingw=false
case "$(uname)" in
  CYGWIN*) cygwin=true ;;
  MINGW*) mingw=true;;
  Darwin*) darwin=true
    # Use /usr/libexec/java_home if available, otherwise fall back to /Library/Java/Home
    # See https://developer.apple.com/library/mac/qa/qa1170/_index.html
    if [ -z "$JAVA_HOME" ]; then
      if [ -x "/usr/libexec/java_home" ]; then
        JAVA_HOME="$(/usr/libexec/java_home)"; export JAVA_HOME
      else
        JAVA_HOME="/Library/Java/Home"; export JAVA_HOME
      fi
    fi
    ;;
esac

if [ -z "$JAVA_HOME" ] ; then
  if [ -r /etc/gentoo-release ] ; then
    JAVA_HOME=$(java-config --jre-home)
  fi
fi

# For Cygwin or MSYS2, ensure paths are in UNIX format before anything is touched
if $cygwin || $mingw ; then
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME=$(cygpath --unix "$JAVA_HOME")
  [ -n "$CLASSPATH" ] &&
    CLASSPATH=$(cygpath --path --unix "$CLASSPATH")
fi

# For Mingw, ensure paths are in UNIX format before anything is touched
if $mingw ; then
  [ -n "$M2_HOME" ] &&
    M2_HOME="$( (cd "$M2_HOME"; pwd -W) | sed 's|/|\\|g')"
fi

if [ -z "$JAVA_HOME" ]; then
  javaExecutable="$(which javac)"
  if [ -n "$javaExecutable" ] && ! [ "$(expr "\"$javaExecutable\"" : '\([^ ]*\)')" = "no" ]; then
    # readlink(1) is not available as standard on Solaris 10.
    readLink=$(which readlink)
    if [ ! "$(expr "$readLink" : '\([^ ]*\)')" = "no" ]; then
      if $darwin ; then
        javaHome="$(dirname "\"$javaExecutable\"")"
        javaExecutable="$(cd "\"$javaHome\"" && pwd -P)/javac"
      else
        javaExecutable="$(readlink -f "\"$javaExecutable\"")"
      fi
      javaHome="$(dirname "\"$javaExecutable\"")"
      javaHome=$(expr "$javaHome" : '\(.*\)/bin')
      JAVA_HOME="$javaHome"
      export JAVA_HOME
    fi
  fi
fi

if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD="$(\unset -f command 2>/dev/null; \command -v java)"
  fi
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly." >&2
  echo "  We cannot execute $JAVACMD" >&2
  exit 1
fi

if [ -z "$JAVA_HOME" ] ; then
  echo "Warning: JAVA_HOME environment variable is not set."
fi

# traverses directory structure from process work directory to filesystem root
# first directory with .mvn subdirectory is considered project base directory
(
  if [ -n "$MAVEN_PROJECTBASEDIR" ] && [ -d "$MAVEN_PROJECTBASEDIR/.mvn" ]; then
    echo "$MAVEN_PROJECTBASEDIR"
    exit 0
  fi
  SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
  while [ "$SCRIPT_DIR" != "/" ] && [ ! -d "$SCRIPT_DIR/.mvn" ]; do
    SCRIPT_DIR="$(dirname "$SCRIPT_DIR")"
  done
  if [ -d "$SCRIPT_DIR/.mvn" ]; then
    echo "$SCRIPT_DIR"
  fi
) | while read projectBasedir; do
  if [ "$projectBasedir" = "" ]; then
    projectBasedir="$(pwd)"
  fi

  # reset MAVEN_OPTS to avoid heredoc accumulation
  MAVEN_PROJECTBASEDIR="$projectBasedir"
  export MAVEN_PROJECTBASEDIR

  if [ -f "$MAVEN_PROJECTBASEDIR/.mvn/jvm.config" ] ; then
    JVM_CONFIG_MAVEN_PROPS=$(tr '\n' ' ' < "$MAVEN_PROJECTBASEDIR/.mvn/jvm.config")
  fi

  WRAPPER_LAUNCHER=org.apache.maven.wrapper.MavenWrapperMain

  WRAPPER_URL="https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/@@project.version@@/maven-wrapper-@@project.version@@.jar"
  WRAPPER_SHA_256_SUM=""

  if [ -f "$MAVEN_PROJECTBASEDIR/.mvn/wrapper/maven-wrapper.properties" ]; then
    while IFS="=" read -r key value; do
      case "$key" in
        wrapperUrl) WRAPPER_URL="$value" ;;
        wrapperSha256Sum) WRAPPER_SHA_256_SUM="$value" ;;
      esac
    done < "$MAVEN_PROJECTBASEDIR/.mvn/wrapper/maven-wrapper.properties"
  fi

  WRAPPER_JAR="$MAVEN_PROJECTBASEDIR/.mvn/wrapper/maven-wrapper.jar"

  if [ -n "$MVNW_VERBOSE" ] && [ -f "$WRAPPER_JAR" ]; then
    echo "Found $WRAPPER_JAR"
  fi

  if [ ! -f "$WRAPPER_JAR" ]; then
    if [ -n "$MVNW_REPOURL" ]; then
      WRAPPER_URL="$MVNW_REPOURL/org/apache/maven/wrapper/maven-wrapper/@@project.version@@/maven-wrapper-@@project.version@@.jar"
    fi

    if [ -n "$MVNW_VERBOSE" ]; then
      echo "Couldn't find $WRAPPER_JAR, downloading it ..."
      echo "Downloading from: $WRAPPER_URL"
    fi

    jarCommand="wget -O \"$WRAPPER_JAR\"" || jarCommand="curl -o \"$WRAPPER_JAR\""
    for i in $(seq 1 3); do
      if [ -n "$(which wget 2>/dev/null)" ]; then
        if wget --no-verbose -O "$WRAPPER_JAR" "$WRAPPER_URL"; then break; fi
      elif [ -n "$(which curl 2>/dev/null)" ]; then
        if curl -f -L -o "$WRAPPER_JAR" "$WRAPPER_URL"; then break; fi
      else
        echo "Error: Neither wget nor curl found." >&2
        exit 1
      fi
      sleep 1
    done

    if [ ! -f "$WRAPPER_JAR" ]; then
      echo "Error: Failed to download $WRAPPER_JAR" >&2
      exit 1
    fi

    if [ -n "$MVNW_VERBOSE" ]; then
      echo "Finished downloading $WRAPPER_JAR"
    fi
  fi

  if [ -n "$WRAPPER_SHA_256_SUM" ]; then
    if [ -n "$(which sha256sum 2>/dev/null)" ]; then
      wrapperChecksum=$(sha256sum "$WRAPPER_JAR" | cut -d ' ' -f 1)
    elif [ -n "$(which shasum 2>/dev/null)" ]; then
      wrapperChecksum=$(shasum -a 256 "$WRAPPER_JAR" | cut -d ' ' -f 1)
    else
      echo "Warning: sha256sum/shasum not found; skipping wrapper jar validation." >&2
    fi
    if [ -n "$wrapperChecksum" ] && [ "$wrapperChecksum" != "$WRAPPER_SHA_256_SUM" ]; then
      echo "Error: Failed to validate Maven wrapper SHA-256, your Maven wrapper might be compromised." >&2
      echo "Investigate or delete $WRAPPER_JAR to attempt a clean download." >&2
      echo "If you updated your Maven version, you need to update the specified wrapperSha256Sum property." >&2
      exit 1
    fi
  fi

  exec "$JAVACMD" \
    $JVM_CONFIG_MAVEN_PROPS \
    $MAVEN_OPTS \
    $MAVEN_DEBUG_OPTS \
    -classpath "$WRAPPER_JAR" \
    "-Dmaven.multiModuleProjectDirectory=$MAVEN_PROJECTBASEDIR" \
    ${WRAPPER_LAUNCHER} $MAVEN_CONFIG "$@"
done
