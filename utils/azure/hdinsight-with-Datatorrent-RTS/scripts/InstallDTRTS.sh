#!/bin/bash

DT_INSTALLER='datatorrent-rts.bin'
DT_INSTALLER_MD5=$DT_INSTALLER'.md5'
DT_DOWNLOADS='https://www.datatorrent.com/downloads/latest'
PORT=$1

DT_PATH=$DT_DOWNLOADS'/'$DT_INSTALLER
DT_PATH_MD5=$DT_DOWNLOADS'/'$DT_INSTALLER_MD5

DT_ADMIN_USER="dtadmin"
DT_ADMIN_USER_GROUP="hadoop"

create_user()
{
  echo "Creating dtadmin user".
  DT_USER=$1
  DT_USER_GROUP=$2
  useradd -m -K "UMASK=022" -g $DT_USER_GROUP $DT_USER
}

download()
{
  FILE_PATH=$1

  echo "Downloading $FILE_PATH";

  curl -LSO $FILE_PATH
  if [[ "$?" != 0 ]]; then
    echo "Error downloading $FILE_PATH"
  else
    echo "Downloaded $FILE_PATH successfully"
  fi
  echo
}

checksum()
{
  FILE=$1
  MD5_FILE=$2

  echo "md5 check between $FILE & $MD5_FILE"

  MD5=$(cat $MD5_FILE)
  echo "md5: $MD5"

  MD5SUM=$(md5sum $FILE | awk '{print $1}')
  echo "md5sum: $MD5SUM"

  if [ "$MD5" != "$MD5SUM" ]
  then
    echo "md5: $MD5 did not match with $MD5SUM"
  else
    echo "md5 checksum match successfully"
  fi
  echo
}

install()
{
  INSTALLER=$1
  GATEWAY_USER=$2
  GATEWAY_GROUP=$3
  GATEWAY_PORT=$4
  echo "Installing $INSTALLER"

  chmod +x $INSTALLER
  /bin/sh $INSTALLER -U $GATEWAY_USER -G $GATEWAY_GROUP -g $GATEWAY_PORT

}

download $DT_PATH
download $DT_PATH_MD5
checksum $DT_INSTALLER $DT_INSTALLER_MD5
create_user $DT_ADMIN_USER $DT_ADMIN_USER_GROUP
install $DT_INSTALLER $DT_ADMIN_USER $DT_ADMIN_USER_GROUP $PORT
#Following restart is required for SPOI-12825
sleep 1m
sudo -H -u $DT_ADMIN_USER dtgateway restart
