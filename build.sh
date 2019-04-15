#!/bin/bash

function die {
    declare MSG="$@"
    echo -e "$0: Error: $MSG">&2
    exit 1
}

appNames=(FirehoseTransformer)

rootAppDir=$(pwd)

for app in ${appNames[@]}; do
    echo "Building app $app"
    cd $app/main || die "dir $app/main does not exist"
    go get
    go build ${app}.go || die "error builing ${app}.go"
    cd $rootAppDir || die "can't cd to $rootAppDir"
done