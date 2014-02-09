#!/bin/bash
if [ -f /etc/lsb-release ]; then
        os=$(lsb_release -s -d | sed 's/"//g')
elif [ -f /etc/debian_version ]; then
        os="Debian $(cat /etc/debian_version)"
elif [ -f /etc/redhat-release ]; then
        os=`cat /etc/redhat-release`
else
        os="$(uname -s) $(uname -r)"
fi
echo $os | cut -d ' ' -f1
