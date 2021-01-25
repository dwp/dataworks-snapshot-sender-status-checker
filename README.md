# dataworks-snapshot-sender-status-checker

## An AWS lambda which receives SQS messages and monitors and reports on the status of a snapshot sender run.

This repo contains Makefile, and Dockerfile to fit the standard pattern.
This repo is a base to create new Docker image repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
`make bootstrap`