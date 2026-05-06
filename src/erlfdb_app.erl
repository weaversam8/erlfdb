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

-module(erlfdb_app).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc hidden.
-endif.

-behaviour(application).

-export([start/2, stop/1]).

%% Options that only apply to the NIF multi-version-client threading model.
%% Workers are single-threaded subprocesses, so these are meaningless and
%% stripped before being sent in the init request.
-define(NIF_ONLY_OPTIONS, [
    external_client_library,
    external_client_directory,
    client_threads_per_version,
    callbacks_on_external_threads
]).

start(_Type, _Args) ->
    ApiVersion = resolve_api_version(),
    ResolvedOpts = resolve_network_options(),

    %% Store the resolved options so callers can inspect them (same semantics
    %% as the NIF path — any code reading network_options_resolved still works).
    application:set_env(erlfdb, network_options_resolved, ResolvedOpts),

    %% Build the {ApiVersion, NormalizedOpts} tuple that will be sent to each
    %% worker subprocess in its init request. Options are pre-converted to
    %% {atom, binary} tuples so the C side needs only ei_decode_binary.
    WorkerOpts = worker_options(ResolvedOpts),
    NormalizedOpts = normalize_options(WorkerOpts),
    application:set_env(erlfdb, init_request_args, {ApiVersion, NormalizedOpts}),

    erlfdb_sup:start_link().

stop(_State) ->
    ok.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

resolve_api_version() ->
    case application:get_env(erlfdb, api_version) of
        {ok, V} when is_integer(V), V > 0 -> V;
        _ -> erlfdb_nif:get_default_api_version()
    end.

resolve_network_options() ->
    Defaults = erlfdb_network_options:get_defaults(),
    Merged =
        case application:get_env(erlfdb, network_options) of
            {ok, UserOpts} when is_list(UserOpts) ->
                erlfdb_network_options:merge(Defaults, UserOpts);
            _ ->
                Defaults
        end,
    %% Follow {M, F, A} tuples to get the actual runtime values.
    Followed = lists:map(
        fun
            ({Name, {M, F, A}}) when
                is_atom(Name), is_atom(M), is_atom(F), is_list(A)
            ->
                {Name, erlang:apply(M, F, A)};
            (Other) ->
                Other
        end,
        Merged
    ),
    %% Drop options explicitly set to false (disabling a default).
    lists:filter(
        fun
            ({_Name, false}) -> false;
            (_) -> true
        end,
        Followed
    ).

%% Strip options that only make sense in the NIF multi-version-client model.
worker_options(ResolvedOpts) ->
    lists:filter(
        fun
            ({Name, _}) -> not lists:member(Name, ?NIF_ONLY_OPTIONS);
            (Name) when is_atom(Name) -> not lists:member(Name, ?NIF_ONLY_OPTIONS)
        end,
        ResolvedOpts
    ).

%% Convert the resolved option list to [{atom, binary}] so the C worker only
%% needs to call ei_decode_binary on each value.
normalize_options(Opts) ->
    lists:map(
        fun
            (Name) when is_atom(Name) ->
                {Name, <<>>};
            ({Name, true}) when is_atom(Name) ->
                {Name, <<>>};
            ({Name, Value}) when is_atom(Name), is_binary(Value) ->
                {Name, Value};
            ({Name, Value}) when is_atom(Name), is_integer(Value) ->
                {Name, <<Value:64/little>>}
        end,
        Opts
    ).
