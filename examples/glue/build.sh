#!/usr/bin/env bash

mkdir deps && cd deps

pip install -r ../requirements.txt -t .

cp -r ../doordahost_glue/ .

zip -r ../doordahost_etl_glue.zip .