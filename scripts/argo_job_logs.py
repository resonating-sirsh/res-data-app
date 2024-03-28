#!/usr/bin/env python3 -m
import res
import sys

# usage: python scripts/argo_job_logs.py ltcsl-532e33f0c8-2022-05-20-12-02-09

s3 = res.connectors.load("s3")
job = sys.argv[1]
for env in ['dev', 'production']:
    path = f"s3://argo-{env}-artifacts/{job}/"
    for file_name in s3.ls(path, suffixes=".log"):
        print("---")
        print("-", file_name)
        print("---")
        print(s3.read(file_name).decode("unicode_escape"))
