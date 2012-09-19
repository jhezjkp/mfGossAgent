#!/bin/bash
kill -9 `ps aux | grep gossAgent | grep -v grep | awk '{print $2 " "}' | tr -d '\n'`
