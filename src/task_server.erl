%%%-------------------------------------------------------------------
%%% @author yangyajun03
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 五月 2018 上午11:17
%%%-------------------------------------------------------------------
-module(task_server).
-author("yangyajun03").

-behaviour(gen_server).

-include_lib("stdlib/include/qlc.hrl").
-include("task_dispatch.hrl").
%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% API
-export([
    mwrite/2,
    mquery/1,
    mdirty_read/2,
    mdelete/1,
    mdelete/2,
    mnesia_node/1
]).

-export([
    start/0,
    task_dispatch/0,
    node_task_dispatch/0,
    task_id/1,

    remove_node_task/1,
    remove_node_task/2,
    fresh_nodes_task/0
]).

-define(SERVER, ?MODULE).
-record(state, {
    normal_fresh
}).

%%%===================================================================
%%% API
%%%===================================================================
start() ->
    gen_server:call(?SERVER, start_dispatch).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    {ok, #state{normal_fresh = true}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({update_fresh_status, FreshStatus}, _From, State) ->
    {reply, ok, State#state{normal_fresh = FreshStatus}};
handle_call(start_dispatch, _From, State) ->
    erlang:send_after(500, self(), check),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(check, #state{normal_fresh = Fresh} = State) ->
    [FirstNode | _Node] = lists:sort(nodes() ++ [node()]),
    case FirstNode == node() andalso Fresh of
        true ->
            try
                check_change()
            catch
                E:R ->
                    error_logger:error_msg("check task change failed, error:~p, reason:~p, st:~p",
                        [E, R, erlang:get_stacktrace()])
            end;
        false ->
            ok
    end,
    erlang:send_after(?CHECK_TIME, self(), check),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%各节点所属的任务，由节点自己保证任务的正常运行
%任务迁移时，先取消占用，才能再分配
check_change() ->
    RunningNode = mnesia_node(running),
    {ok, TaskInfo} = mquery(dispatch_task),
    LastDispatchNodes = lists:foldl(fun(T, Acc) ->
        {_Table, _Id, Node, _Task} = T,
        case lists:member(Node, Acc) of
            true ->
                Acc;
            false ->
                Acc ++ [Node]
        end end, [], TaskInfo),

    RunningTaskSize = length(TaskInfo),
    TaskModule = ?TASK_MODULE,
    {ok, TaskList} = TaskModule:task_list(?NODE_TIMEOUT),
    TaskSize = length(TaskList),
    %节点数变更
    case lists:sort(LastDispatchNodes) == lists:sort(RunningNode) andalso TaskSize == RunningTaskSize of
        true ->
            ignore;
        false ->
            dispatch_task(RunningNode, LastDispatchNodes, TaskInfo)
    end.

%不存在已经分配的任务时，任务平均分配到各个运行节点
dispatch_task(RunningNode, [], _TaskInfo) ->
    TaskModule = ?TASK_MODULE,
    {ok, TaskList} = TaskModule:task_list(?NODE_TIMEOUT),
    GroupSize = case length(RunningNode) == 1 of
                    true ->
                        length(TaskList) div length(RunningNode);
                    false ->
                        case length(TaskList) rem 2 == 0 of
                            true ->
                                length(TaskList) div length(RunningNode);
                            false ->
                                length(TaskList) div length(RunningNode) + 1
                        end
                end,
    GroupList = group_task(GroupSize, TaskList, []),

    lists:zipwith(fun(Node, Tasks) -> task_cmd(Node, Tasks, task_notice) end, RunningNode, GroupList);

%任务按照最少迁移原则重新分配
dispatch_task(RunningNode, LastDispatchNodes, TaskInfo) ->
    TaskInfoList = convert_list(TaskInfo),
    %新启动的节点
    NeedDispatchNodes = RunningNode -- LastDispatchNodes,

    %未运行的节点
    StopRunningNodes = LastDispatchNodes -- RunningNode,

    error_logger:info_msg("running_nodes:~p, last_dispatch_nodes:~p need_dispatch_nodes:~p,
    stop_running_nodes:~p",[RunningNode, LastDispatchNodes, NeedDispatchNodes, StopRunningNodes]),

    %删除未运行节点的任务记录
    stop_node_task_clean(StopRunningNodes, TaskInfo),

    %未运行任务列表
    TaskModule = ?TASK_MODULE,
    {ok, TaskList} = TaskModule:task_list(?NODE_TIMEOUT),
    GroupSize = case length(RunningNode) == 1 of
                    true ->
                        length(TaskList) div length(RunningNode);
                    false ->
                        case length(TaskList) rem 2 == 0 of
                            true ->
                                length(TaskList) div length(RunningNode);
                            false ->
                                length(TaskList) div length(RunningNode) + 1
                        end
                end,
    %停止节点的任务列表 + 之前分配没有功能的任务列表
    NormalNodes = LastDispatchNodes -- StopRunningNodes,
    StopRunningTasks = TaskList -- lists:foldl(fun(NormalNode, Acc) ->
        Acc ++ proplists:get_value(NormalNode, TaskInfoList, []) end, [], NormalNodes),

    error_logger:info_msg("normal_nodes:~p, group_size:~p, stop_running_tasks:~p, convert_table:~p", [
        NormalNodes, GroupSize, StopRunningTasks, TaskInfoList]),
    case lists:sort(LastDispatchNodes) == lists:sort(RunningNode) of
        true ->
            %节点未变更，存在未分配成功的任务
            add_dispatch(RunningNode, GroupSize, StopRunningTasks, TaskInfoList);
        false ->
            %节点有变更
            case NeedDispatchNodes == [] of
                true ->
                    %节点数变小
                    error_logger:info_msg("add_dispatch"),
                    add_dispatch(RunningNode, GroupSize, StopRunningTasks, TaskInfoList);
                false ->
                    %节点数变大
                    error_logger:info_msg("subtract_dispatch"),
                    subtract_dispatch(RunningNode, GroupSize, StopRunningTasks, TaskInfoList)
            end
    end.

convert_list(TaskInfo) ->
    lists:foldl(fun(T, Acc) ->
        {_Table, _Id, Node, Task} = T,
        case proplists:get_value(Node, Acc) of
            undefined ->
                Acc ++ [{Node, [Task]}];
            ValueList ->
                lists:keyreplace(Node, 1, Acc, {Node, ValueList ++ [Task]})
        end end, [], TaskInfo).

add_dispatch(_RunningNodes, _GroupSize, [], _TaskInfoList) ->
    ok;
add_dispatch([], _GroupSize, _StopRunningTasks, _TaskInfoList) ->
    ok;
add_dispatch(RunningNodes, GroupSize, StopRunningTasks, TaskInfoList) ->
    [FirstNode | OtherNodes] = RunningNodes,
    case proplists:get_value(FirstNode, TaskInfoList) of
        undefined ->
            error_logger:error_msg("exception exist !!!!!!"),
            add_dispatch(OtherNodes, GroupSize, StopRunningTasks, TaskInfoList);
        VList ->
            DiffValue = GroupSize - length(VList),
            case DiffValue > 0 of
                true ->
                    {HeadList, TailList} = lists:split(DiffValue, StopRunningTasks),
                    task_cmd(FirstNode, HeadList, task_notice),
                    add_dispatch(OtherNodes, GroupSize, TailList, TaskInfoList);
                false ->
                    add_dispatch(OtherNodes, GroupSize, StopRunningTasks, TaskInfoList)
            end
    end.

subtract_dispatch([], _GroupSize, _StopRunningTasks, _TaskInfoList) ->
    ok;
subtract_dispatch(RunningNodes, GroupSize, StopRunningTasks, TaskInfoList) ->
    [FirstNode | OtherNodes] = RunningNodes,
    case proplists:get_value(FirstNode, TaskInfoList) of
        undefined ->
            %新启动的节点
            RemainStopRunningTasks = case GroupSize > length(StopRunningTasks) of
                                         true ->
                                             task_cmd(FirstNode, StopRunningTasks, task_notice),
                                             [];
                                         false ->
                                             {HeadList, TailList} = lists:split(GroupSize, StopRunningTasks),
                                             task_cmd(FirstNode, HeadList, task_notice),
                                             TailList
                                     end,
            subtract_dispatch(OtherNodes, GroupSize, RemainStopRunningTasks, TaskInfoList);
        VList ->
            DiffValue = length(VList) - GroupSize,
            case DiffValue > 0 of
                true ->
                    {HeadList, _TailList} = lists:split(DiffValue, VList),
                    task_cmd(FirstNode, HeadList, task_cancel),
                    subtract_dispatch(OtherNodes, GroupSize, StopRunningTasks, TaskInfoList);
                false ->
                    subtract_dispatch(OtherNodes, GroupSize, StopRunningTasks, TaskInfoList)
            end
    end.

task_cmd(NoticeNode, TaskList, Cmd) ->
    case NoticeNode == node() of
        true ->
            %本节点任务
            local_task(?TASK_MODULE, Cmd, TaskList);
        false ->
            [remote_task(NoticeNode, ?TASK_MODULE, Cmd, [TaskInfo, ?NODE_TIMEOUT], ?Retry) || TaskInfo <- TaskList]
    end.

%删除节点的任务记录
stop_node_task_clean(Nodes, TaskInfo) ->
    lists:foreach(fun(T) ->
        {_Table, Id, Node, Task} = T,
        case lists:member(Node, Nodes) of
            true ->
                error_logger:info_msg("clean node task, node:~p, task:~p", [Node, Task]),
                ensure_mdelete(dispatch_task, Id, ?Retry);
            false ->
                ignore
        end end, TaskInfo).

local_task(TaskModule, Function, TaskList) ->
    TaskModule = ?TASK_MODULE,
    Fun = fun(TaskInfo) ->
        case TaskModule:Function(TaskInfo, ?NODE_TIMEOUT) of
            ok ->
                %设置节点任务信息
                case Function of
                    task_notice ->
                        ensure_mwrite(dispatch_task, {dispatch_task, task_id(TaskInfo), node(), TaskInfo}, ?Retry);
                    task_cancel ->
                        ensure_mdelete(dispatch_task, task_id(TaskInfo), ?Retry)
                end;
            error ->
                ignore
        end
          end,
    [Fun(TaskInfo) || TaskInfo <- TaskList].

task_id(TaskInfo) ->
    Sig = erlang:md5(term_to_binary(TaskInfo)),
    iolist_to_binary([io_lib:format("~2.16.0b", [S]) || S <- binary_to_list(Sig)]).

remote_task(_Node, _Module, _Function, _Args, 0) ->
    error;
remote_task(Node, Module, Function, Args, Retry) ->
    [TaskInfo, _TimeOut] = Args,
    case rpc:call(Node, Module, Function, Args, ?NODE_TIMEOUT) of
        {badrpc, Reason} ->
            error_logger:error_msg("rpc node:~p, module:~p, function:~p, args:~p failed, reason:~p",
                [Node, Module, Function, Args, Reason]),
            remote_task(Node, Module, Function, Args, Retry - 1);
        ok ->
            error_logger:info_msg("rpc node:~p, module:~p, function:~p, args:~p success",
                [Node, Module, Function, Args]),
            %设置节点任务信息
            case Function of
                task_notice ->
                    ensure_mwrite(dispatch_task, {dispatch_task, task_id(TaskInfo), Node, TaskInfo}, ?Retry);
                task_cancel ->
                    ensure_mdelete(dispatch_task, task_id(TaskInfo), ?Retry)
            end
    end.

group_task(_GroupSize, [], GroupList) ->
    GroupList;
group_task(GroupSize, TaskList, GroupList) ->
    {FirstList, RemainList} = lists:split(GroupSize, TaskList),
    group_task(GroupSize, RemainList, GroupList ++ [FirstList]).

mwrite(Table, Record) ->
    F = fun() ->
        mnesia:write(Record)
        end,
    case mnesia:transaction(F) of
        {atomic, _R} ->
            error_logger:info_msg("mnesia save table:~p, record:~p", [Table, Record]),
            ok;
        {aborted, Reason} ->
            error_logger:error_msg("mnesia save error:~p, table:~p, record:~p", [Reason, Table, Record]),
            error
    end.

ensure_mwrite(_Table, _Record, 0) ->
    error;
ensure_mwrite(Table, Record, Retry) ->
    case mwrite(Table, Record) of
        ok ->
            ok;
        error ->
            ensure_mwrite(Table, Record, Retry - 1)
    end.

mquery(Table) ->
    F = fun() ->
        Q = qlc:q([E || E <- mnesia:table(Table)]),
        qlc:e(Q)
        end,
    case mnesia:transaction(F) of
        {atomic, List} ->
            {ok, List};
        {aborted, Reason} ->
            error_logger:error_msg("mnesia query error:~p, table:~p", [Reason, Table]),
            {error, Reason}
    end.

mdelete(Table, Key) ->
    F = fun() ->
        mnesia:delete({Table, Key})
        end,
    case mnesia:transaction(F) of
        {atomic, _R} ->
            error_logger:info_msg("mnesia delete table:~p, key:~p", [Table, Key]),
            ok;
        {aborted, Reason} ->
            error_logger:error_msg("mnesia delete table:~p, key:~p failed, reason:~p", [Table, Key, Reason]),
            error
    end.

ensure_mdelete(_Table, _Key, 0) ->
    error;
ensure_mdelete(Table, Key, Retry) ->
    case mdelete(Table, Key) of
        ok ->
            ok;
        error ->
            ensure_mdelete(Table, Key, Retry - 1)
    end.

mdelete(Table) ->
    {ok, List} = mquery(Table),
    [mdelete(Table, Key) || {_Table, Key, _Value} <- List].

mdirty_read(Table, Key) ->
    case mnesia:dirty_read({Table, Key}) of
        [] ->
            {error, empty};
        [{Table, Key, Cfg}] ->
            {ok, Cfg};
        Error ->
            {error, Error}
    end.

mnesia_node(running) ->
    mnesia:system_info(running_db_nodes);
mnesia_node(stop) ->
    AllNodes = mnesia:system_info(db_nodes),
    RunningNodes = mnesia:system_info(running_db_nodes),
    AllNodes -- RunningNodes.


node_task_dispatch() ->
    {ok, TaskInfo} = mquery(dispatch_task),
    convert_list(TaskInfo).

task_dispatch() ->
    mquery(dispatch_task).

remove_node_task(Node) ->
    TaskList = node_task_dispatch(),
    NodeTask = proplists:get_value(Node, TaskList, []),
    task_cmd(Node, NodeTask, task_cancel).

remove_node_task(Node, TaskInfo) ->
    task_cmd(Node, [TaskInfo], task_cancel).

fresh_nodes_task() ->
    gen_server:call(?SERVER, {update_fresh_status, false}),
    Nodes = nodes() ++ [node()],
    [remove_node_task(Node) || Node <- Nodes],
    gen_server:call(?SERVER, {update_fresh_status, true}).
