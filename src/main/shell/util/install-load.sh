#!/bin/bash
set -e

# Usage:
#  --user-home - an alternative user to install into, default /home/hadoop
#  --tmpdir - an alternative temporary directory, default TMPDIR or /tmp if not set
#  --no-bash - do not update .bashrc

LATEST=http://@location@/load/@majorVersion@/latest.txt

case "`uname`" in
  Darwin)
    USER_HOME=/Users/${USER};;
  *)
    USER_HOME=/home/${USER};;
esac

INSTALL_ON_SLAVES=false

IS_MASTER=true
if [ -f /mnt/var/lib/info/instance.json ]
then
  # spit out debug info if run from a bootstrap action
  set -x
  IS_MASTER=`cat /mnt/var/lib/info/instance.json | tr -d '\n ' | sed -n 's|.*\"isMaster\":\([^,]*\).*|\1|p'`
  USER_HOME=/home/hadoop
fi

# root of install, by default becomes $USER_HOME/.load
INSTALL_PATH=$USER_HOME

BASH_PROFILE=.bashrc
UPDATE_BASH=true

# don't install twice
[ -n "`which load`" ] && UPDATE_BASH=false

[ -z "$TMPDIR" ] && TMPDIR=/tmp

error_msg () # msg
{
  echo 1>&2 "Error: $1"
}

error_exit () # <msg> <cod>
{
  error_msg "$1"
  exit ${2:-1}
}

while [ $# -gt 0 ]
do
  case "$1" in
    --user-home)
      shift
      USER_HOME=$1
      ;;
    --tmpdir)
      shift
      TMPDIR=$1
      ;;
    --no-bash)
      UPDATE_BASH=false
      ;;
    --slaves)
      INSTALL_ON_SLAVES=true
      ;;
    -*)
      # do not exit out, just note failure
      error_msg "unrecognized option: $1"
      ;;
    *)
      break;
      ;;
  esac
  shift
done

if [ ! $IS_MASTER ] && [ ! $INSTALL_ON_SLAVES ] 
then
  echo "This is neither the master nor the slaves. Nothing to install."
  exit 0
fi

[ -z ${LOAD_HOME} ] && LOAD_HOME=${INSTALL_PATH}/.load

REDIR=$TMPDIR/latest
ARCHIVE=$TMPDIR/archive.tgz
UNARCHIVED=$TMPDIR/unarchived/

[ -d "${UNARCHIVED}" ] && rm -rf ${UNARCHIVED}

# find latest multitool
LATEST_LOAD=`curl --connect-timeout 10 --retry 5 ${LATEST}`
echo ${LATEST_LOAD} > ${REDIR}

[ -f ${LOAD_HOME}/latest ] && LOAD_CURRENT=`cat ${LOAD_HOME}/latest`
# force update if on dev releases
if [ "`cat $REDIR`" = "${LOAD_CURRENT/wip-dev/}" ]; then
  echo "no update available"
  exit 0
fi

# download latest
curl --connect-timeout 10 --retry 5 -o ${ARCHIVE} ${LATEST_LOAD} 

# unpack into /usr/local/<multitool home>
mkdir -p ${UNARCHIVED}
tar -xzf ${ARCHIVE} -C ${UNARCHIVED}

# move existing out of the way
if [ -d "${LOAD_HOME}" ]; then
  mv ${LOAD_HOME} $TMPDIR/$RANDOM
fi

#if LOAD_HOME does not exist, create it
if [ ! -d "${LOAD_HOME}" ]; then
    mkdir -p ${LOAD_HOME}
    chmod a+r ${LOAD_HOME}
fi

cp -r ${UNARCHIVED}/load-*/* ${LOAD_HOME}/


cp ${REDIR} ${LOAD_HOME}/latest

echo "Successfully installed Load into \"${LOAD_HOME}\"."
echo "See \"${LOAD_HOME}/docs\" for documentation."

if [ "${UPDATE_BASH}" = "true" -a -w "${USER_HOME}/${BASH_PROFILE}" ]; then
cat >> ${USER_HOME}/${BASH_PROFILE} <<- EOF

# Cascading Load - Concurrent, Inc.
# http://cascading.org/load

export LOAD_HOME=${LOAD_HOME}

# add load to PATH
export PATH=\$PATH:\$LOAD_HOME/bin

EOF

  echo "Successfully updated ${USER_HOME}/${BASH_PROFILE} with new PATH information."
elif [ -z "`which multitool`" ]; then

  echo "To complete installation, add \"${LOAD_HOME}/bin\" to the PATH."
fi

CASCADING_CONFIG_DIR=$HOME/.cascading

if [[ ! -e  ${CASCADING_CONFIG_DIR} ]]; then
  mkdir ${CASCADING_CONFIG_DIR}
fi

CASCADING_CONFIG_FILE=${CASCADING_CONFIG_DIR}/default.properties

HADOOP_MAJOR_VERSION=`hadoop version | grep "Hadoop" | awk '{print $2 }' | awk -F\. '{print $1}'`
if [[ "2" == $HADOOP_MAJOR_VERSION ]]; then
  echo 'load.platform.name=hadoop2-mr1' >> $CASCADING_CONFIG_FILE
else
  echo 'load.platform.name=hadoop' >> $CASCADING_CONFIG_FILE
fi
