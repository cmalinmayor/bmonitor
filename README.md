# BMonitor: A Python Tool for Monitoring Bsub Jobs

This library includes a few basic functions for monitoring bsub jobs,
allowing a python programmer to create programmatic workflows rather
than doing error checking and resubmission of failed jobs by hand.

At a small scale, it is easy enough to run and check the status of 
each bsub job individually. However, when running a larger quantity 
of sequential jobs, and especially array jobs, it is very helpful
to be able to detect and potentially resolve errors programmatically.
This library is designed to provide functions that can parse bsub
output (especially the 'bjobs' command) and report job status in 
a way that can be used by a script, rather than a human.
