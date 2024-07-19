#!/bin/bash

URL="https://rodenli-1308597516.cos.ap-guangzhou.myqcloud.com/trino/trino_keystore.jks"
DEST_DIR="/usr/local/service/trino/etc/"
FILE_NAME="trino_keystore.jks"
GROUP_NAME="hadoop"

# 下载文件
echo "Downloading file from $URL..."
wget -P $DEST_DIR $URL

# 设置文件的用户组
echo "Setting file group to $GROUP_NAME..."
chown $GROUP_NAME:$GROUP_NAME $DEST_DIR/$FILE_NAME

echo "File download and group setting complete."