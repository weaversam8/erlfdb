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

%% Port-based replacement for erlfdb_nif. Functions here route requests
%% through an erlfdb_worker gen_server to the erlfdb_worker subprocess.
%%
%% Only get_max_api_version/0 is implemented in step 3; remaining operations
%% are added in subsequent steps.

-module(erlfdb_port).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc hidden.
-endif.

-export([
    get_max_api_version/0
]).

-spec get_max_api_version() -> integer().
get_max_api_version() ->
    ok = ensure_started(),
    Worker = erlfdb_worker_sup:pick_worker(),
    case gen_server:call(Worker, {request, get_max_api_version, {}}) of
        {ok, Vsn} when is_integer(Vsn) ->
            Vsn;
        {error, Reason} ->
            erlang:error({erlfdb_error, Reason})
    end.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

ensure_started() ->
    case application:ensure_started(erlfdb) of
        ok -> ok;
        {error, Reason} -> erlang:error({erlfdb_start_failed, Reason})
    end.
