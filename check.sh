#!/bin/sh
#更新程序脚本
# your repository folder
path=`pwd`
if [ $# != 0 ]; then
    $path=$1
fi
cd $path

# fetch changes, git stores them in FETCH_HEAD
git fetch

# check for remote changes in origin repository
newUpdatesAvailable=`git diff HEAD FETCH_HEAD`
if [ "$newUpdatesAvailable" != "" ]
then
    #check if the fallback branch exists
    if [ `git branch | grep fallbacks` != "" ]; then
        echo "fallbacks branch exits"
        git checkout fallbacks
        git merge master
        echo "meger current master"
    else
        # create the fallback
        git branch fallbacks

        git checkout fallbacks

        git add .
        git add -u
        git commit -m `date "+%Y-%m-%d"`
        echo "fallback created"
    fi

    git checkout master
    git merge FETCH_HEAD
    echo "merged updates"
    else
    echo "no updates available"
fi
