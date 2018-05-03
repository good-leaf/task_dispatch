-module(task_dispatch_app).
-behaviour(application).

-include("task_dispatch.hrl").
-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	check_fun_export(),
	task_dispatch_sup:start_link().

stop(_State) ->
	ok.

check_fun_export() ->
	true = erlang:function_exported(?TASK_MODULE, task_list, 1),
	true = erlang:function_exported(?TASK_MODULE, task_notice, 2),
	true = erlang:function_exported(?TASK_MODULE, task_cancel, 2).