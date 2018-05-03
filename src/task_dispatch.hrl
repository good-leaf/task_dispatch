%%%-------------------------------------------------------------------
%%% @author yangyajun03
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 五月 2018 下午1:36
%%%-------------------------------------------------------------------
-author("yangyajun03").

-define(APP_NAME, task_dispatch).
-define(TASK_MODULE, begin 	{ok, TaskModule} = application:get_env(?APP_NAME, task_module), TaskModule end).
-define(NODE_TIMEOUT, application:get_env(?APP_NAME, node_timeout, 10000)).
-define(CHECK_TIME, application:get_env(?APP_NAME, check_time, 1000)).
-define(Retry, application:get_env(?APP_NAME, retry, 3)).