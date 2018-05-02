PROJECT = task_dispatch
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = mnesia_cluster
dep_mnesia_cluster = git https://github.com/good-leaf/mnesia_cluster.git 1.0.0

include erlang.mk
