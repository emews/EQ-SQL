#!/bin/bash
set -eu

# TEST SWIFT 1

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python

swift-t -I $EQ_SQL/swift $THIS/test-swift-1.swift
