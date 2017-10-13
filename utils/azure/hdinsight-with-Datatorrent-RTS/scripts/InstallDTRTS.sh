#!/bin/bash

DT_VERSION='3.9.1'
DT_INSTALLER='datatorrent-rts-'$DT_VERSION'.bin'
DT_INSTALLER_MD5=$DT_INSTALLER'.md5'
DT_DOWNLOADS='https://www.datatorrent.com/downloads'
PORT=$1

DT_PATH=$DT_DOWNLOADS'/'$DT_VERSION'/'$DT_INSTALLER
DT_PATH_MD5=$DT_DOWNLOADS'/'$DT_VERSION'/'$DT_INSTALLER_MD5

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
  GATEWAY_PORT=$2
  echo "Installing $INSTALLER"

  chmod +x $INSTALLER
  /bin/sh $INSTALLER -g $GATEWAY_PORT

  echo
}

download $DT_PATH
download $DT_PATH_MD5
checksum $DT_INSTALLER $DT_INSTALLER_MD5
install $DT_INSTALLER $PORT

