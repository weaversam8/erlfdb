% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(erlfdb_worker).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc hidden.
-endif.

-behaviour(gen_server).

-export([start_link/1, os_pid/1]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(HELLO_TIMEOUT_MS, 5000).
-define(INIT_TIMEOUT_MS, 5000).

-record(state, {
    worker_ix :: pos_integer(),
    port :: port(),
    os_pid :: integer(),
    %% Monotonic id used to correlate request/reply frames.
    next_req_id = 1 :: pos_integer(),
    %% Map of ReqId -> {gen_server From} for outstanding synchronous calls.
    inflight = #{} :: #{pos_integer() => gen_server:from()}
}).

start_link(WorkerIx) when is_integer(WorkerIx), WorkerIx > 0 ->
    gen_server:start_link(?MODULE, [WorkerIx], []).

%% Returns the OS pid of the worker subprocess. Used by the crash test.
os_pid(WorkerPid) when is_pid(WorkerPid) ->
    gen_server:call(WorkerPid, os_pid).

init([WorkerIx]) ->
    process_flag(trap_exit, true),
    BinPath = worker_executable_path(),
    Port = erlang:open_port(
        {spawn_executable, BinPath},
        [
            binary,
            {packet, 4},
            exit_status,
            use_stdio,
            {args, [integer_to_list(WorkerIx)]}
        ]
    ),
    case wait_for_hello(Port, ?HELLO_TIMEOUT_MS) of
        {ok, OsPid} ->
            InitArgs =
                case application:get_env(erlfdb, init_request_args) of
                    {ok, Args} -> Args;
                    undefined -> {erlfdb_nif:get_default_api_version(), []}
                end,
            case run_init_handshake(Port, InitArgs, ?INIT_TIMEOUT_MS) of
                ok ->
                    %% Advertise this pid so erlfdb_port can route without
                    %% consulting the supervisor. Overwrites any stale entry
                    %% from a previous incarnation of this worker index.
                    ets:insert(erlfdb_workers, {WorkerIx, self()}),
                    {ok, #state{
                        worker_ix = WorkerIx,
                        port = Port,
                        os_pid = OsPid,
                        next_req_id = 2
                    }};
                {error, Reason} ->
                    safe_close_port(Port),
                    {stop, {init_failed, Reason}}
            end;
        {error, Reason} ->
            safe_close_port(Port),
            {stop, {hello_failed, Reason}}
    end.

handle_call(os_pid, _From, State) ->
    {reply, State#state.os_pid, State};
handle_call({request, Op, Args}, From, State) ->
    ReqId = State#state.next_req_id,
    Frame = term_to_binary({req, ReqId, Op, Args}),
    true = port_command(State#state.port, Frame),
    Inflight = maps:put(ReqId, From, State#state.inflight),
    {noreply, State#state{next_req_id = ReqId + 1, inflight = Inflight}};
handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Port, {exit_status, Status}}, #state{port = Port} = State) ->
    {stop, {worker_exited, Status}, State};
handle_info({'EXIT', Port, Reason}, #state{port = Port} = State) ->
    {stop, {port_exit, Reason}, State};
handle_info({Port, {data, Bin}}, #state{port = Port} = State) ->
    try binary_to_term(Bin) of
        {reply, ReqId, Result} ->
            case maps:take(ReqId, State#state.inflight) of
                {From, Inflight} ->
                    gen_server:reply(From, Result),
                    {noreply, State#state{inflight = Inflight}};
                error ->
                    %% No caller waiting (e.g. the init handshake reply, which
                    %% is consumed synchronously in wait_for_hello/run_init_handshake
                    %% before the gen_server loop starts).
                    {noreply, State}
            end;
        _Other ->
            {noreply, State}
    catch
        error:_ ->
            {noreply, State}
    end;
handle_info(_Other, State) ->
    {noreply, State}.

terminate(_Reason, #state{port = Port}) ->
    %% Closing the port causes the worker subprocess to read EOF on stdin and
    %% exit cleanly. If the BEAM crashes hard, the kernel closes our pipe ends
    %% on process death and the worker exits the same way.
    safe_close_port(Port),
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------------
%% Internal helpers
%% --------------------------------------------------------------------------

worker_executable_path() ->
    PrivDir = priv_dir(),
    filename:join(PrivDir, "erlfdb_worker").

priv_dir() ->
    case code:priv_dir(erlfdb) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end.

%% Wait for the worker's startup `{hello, OsPid}` frame.
wait_for_hello(Port, TimeoutMs) ->
    receive
        {Port, {data, Bin}} ->
            try binary_to_term(Bin) of
                {hello, OsPid} when is_integer(OsPid) ->
                    {ok, OsPid};
                Other ->
                    {error, {unexpected_hello, Other}}
            catch
                error:Reason ->
                    {error, {hello_decode_error, Reason}}
            end;
        {Port, {exit_status, Status}} ->
            {error, {worker_exited_before_hello, Status}};
        {'EXIT', Port, Reason} ->
            {error, {port_exit_before_hello, Reason}}
    after TimeoutMs ->
        {error, hello_timeout}
    end.

%% Send `{req, 1, init, {ApiVersion, [{Name, BinVal}, ...]}}` and wait for
%% `{reply, 1, ok}`. InitArgs is the `{ApiVersion, NormalizedOpts}` tuple
%% resolved by erlfdb_app:start/2.
run_init_handshake(Port, InitArgs, TimeoutMs) ->
    ReqId = 1,
    Frame = term_to_binary({req, ReqId, init, InitArgs}),
    true = port_command(Port, Frame),
    receive
        {Port, {data, ReplyBin}} ->
            try binary_to_term(ReplyBin) of
                {reply, ReqId, ok} ->
                    ok;
                Other ->
                    {error, {unexpected_init_reply, Other}}
            catch
                error:Reason ->
                    {error, {init_decode_error, Reason}}
            end;
        {Port, {exit_status, Status}} ->
            {error, {worker_exited_during_init, Status}};
        {'EXIT', Port, Reason} ->
            {error, {port_exit_during_init, Reason}}
    after TimeoutMs ->
        {error, init_timeout}
    end.

safe_close_port(undefined) ->
    ok;
safe_close_port(Port) when is_port(Port) ->
    try
        true = erlang:port_close(Port),
        ok
    catch
        error:badarg ->
            ok
    end.
