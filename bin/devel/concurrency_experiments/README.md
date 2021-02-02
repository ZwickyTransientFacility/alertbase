# s3 concurrency experiments

This holds some experiments for communicating with S3 concurrently.

500 alerts:

run_in_processes.py: 2m37.9s
run_in_threadpool.py: 2m4.7s
run_in_queue.py: 2m22.5s

estimate of linear execution: ~2-3s each, 1500s==25 minutes

conclusion: async is plenty good enough for me
