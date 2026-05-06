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

%% Manages the persistent_term cache that maps (ClusterFile, SchedulerId) to
%% port-backed database handles, mirroring the role of the direct
%% persistent_term writes in erlfdb.erl:open_with_key/2 for the NIF path.
%%
%% The gen_server serialises cache writes to eliminate the race where two
%% concurrent callers both see a cache miss and both call create_database.
%% Reads bypass the gen_server entirely (persistent_term:get is O(1) and
%% lock-free from the reader side).
%%
%% Step 9 will add worker-death monitoring so stale cache entries are pruned
%% when a worker restarts.

-module(erlfdb_worker_pool).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc hidden.
-endif.

-behaviour(gen_server).

-export([start_link/0, open/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Returns a port-backed database handle for the given ClusterFile, creating
%% and caching one if it does not already exist. The persistent_term key
%% mirrors the shape used by erlfdb.erl:open_dist_key/2 so both code paths
%% share the same cache namespace and won't interfere.
-spec open(ClusterFile :: binary(), Options :: list()) ->
    {erlfdb_database, pid(), reference()}.
open(ClusterFile, Options) ->
    Key = dist_key(ClusterFile, Options),
    case persistent_term:get(Key, undefined) of
        undefined ->
            %% Serialise creation through the gen_server to prevent duplicate
            %% creates under concurrent first-open calls for the same key.
            gen_server:call(?MODULE, {open, Key, ClusterFile});
        Db ->
            Db
    end.

%% ---------------------------------------------------------------------------
%% gen_server callbacks
%% ---------------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call({open, Key, ClusterFile}, _From, State) ->
    %% Re-check under the gen_server lock: another caller may have won
    %% the race between the persistent_term miss above and this call.
    Db =
        case persistent_term:get(Key, undefined) of
            undefined ->
                NewDb = erlfdb_port:create_database(ClusterFile),
                persistent_term:put(Key, NewDb),
                NewDb;
            Existing ->
                Existing
        end,
    {reply, Db, State};
handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

%% Mirror of erlfdb.erl:open_dist_key/2 — same key shape so the two code
%% paths (NIF and port) share the same persistent_term namespace.
dist_key(ClusterFile, Options) ->
    case proplists:get_value(dist, Options, scheduler_id) of
        scheduler_id ->
            {erlfdb, open, scheduler_id,
             {ClusterFile, erlang:system_info(scheduler_id)}};
        cluster_file ->
            {erlfdb, open, cluster_file, ClusterFile}
    end.
