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

-module(erlfdb_worker_sup).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc hidden.
-endif.

-behaviour(supervisor).

-export([start_link/0, init/1, worker_count/0, pick_worker/0]).

-define(WORKER_TABLE, erlfdb_workers).

-define(WORKER_COUNT_MAX_RATIO, 8).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Named ETS table maps WorkerIx -> pid. Owned by this supervisor, so it
    %% is automatically destroyed if the supervisor crashes and recreated fresh
    %% when it restarts. Workers register themselves after their handshake
    %% completes and overwrite on restart, so no explicit delete is needed.
    ets:new(?WORKER_TABLE, [named_table, public, set, {read_concurrency, true}]),
    N = worker_count(),
    Schedulers = erlang:system_info(schedulers_online),
    MaxRatio =
        case application:get_env(erlfdb, worker_count_max_ratio) of
            {ok, R} when is_integer(R), R > 0 -> R;
            _ -> ?WORKER_COUNT_MAX_RATIO
        end,
    case N > Schedulers * MaxRatio of
        true ->
            erlang:error(
                {worker_count_exceeds_cap, [
                    {requested, N},
                    {schedulers_online, Schedulers},
                    {max_ratio, MaxRatio}
                ]}
            );
        false ->
            ok
    end,
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    Children = [
        #{
            id => {erlfdb_worker, Ix},
            start => {erlfdb_worker, start_link, [Ix]},
            restart => permanent,
            shutdown => 5000,
            type => worker
        }
     || Ix <- lists:seq(1, N)
    ],
    {ok, {SupFlags, Children}}.

%% Returns the pid of the worker assigned to the current scheduler. Distributes
%% across N workers by mapping scheduler_id (1..schedulers_online) into the
%% 1..N index range. If the entry is stale (worker crashed, not yet restarted),
%% the caller will get a noproc error from gen_server:call - handled in later
%% steps.
pick_worker() ->
    N = worker_count(),
    Ix = ((erlang:system_info(scheduler_id) - 1) rem N) + 1,
    case ets:lookup(?WORKER_TABLE, Ix) of
        [{Ix, Pid}] -> Pid;
        [] -> error(no_worker_available)
    end.

%% Resolves the configured worker count.
%%
%% Allowed env shapes:
%%   - {worker_count, Integer}        - exact count
%%   - {worker_count, schedulers_online} - one per online scheduler (default)
%%   - {worker_count, {M, F, A}}      - call back to compute the count
worker_count() ->
    case application:get_env(erlfdb, worker_count) of
        {ok, N} when is_integer(N), N > 0 ->
            N;
        {ok, schedulers_online} ->
            erlang:system_info(schedulers_online);
        {ok, {M, F, A}} when is_atom(M), is_atom(F), is_list(A) ->
            V = erlang:apply(M, F, A),
            true = is_integer(V) andalso V > 0,
            V;
        _ ->
            erlang:system_info(schedulers_online)
    end.
