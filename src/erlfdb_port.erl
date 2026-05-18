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

%% Port-based replacement for erlfdb_nif.  Functions here route requests
%% through an erlfdb_worker gen_server to the erlfdb_worker subprocess.

-module(erlfdb_port).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc hidden.
-endif.

% Defined at compile time via `erl_opts` in `rebar.config.script`
-define(DEFAULT_API_VERSION, ?erlfdb_compile_time_api_version).

-export([
    get_default_api_version/0,
    get_max_api_version/0,

    future_cancel/1,
    future_silence/1,
    future_is_ready/1,
    future_get_error/1,
    future_get/1,
    wait/1,

    create_database/1,
    database_open_tenant/2,
    database_set_option/2,
    database_set_option/3,
    database_create_transaction/1,
    database_get_main_thread_busyness/1,
    database_get_client_status/1,
    tenant_create_transaction/1,

    transaction_set_option/2,
    transaction_set_option/3,
    transaction_set_read_version/2,
    transaction_get_read_version/1,
    transaction_get/3,
    transaction_get_estimated_range_size/3,
    transaction_get_key/3,
    transaction_get_addresses_for_key/2,
    transaction_get_range/9,
    transaction_get_range_split_points/4,
    transaction_get_mapped_range/10,
    transaction_set/3,
    transaction_clear/2,
    transaction_clear_range/3,
    transaction_atomic_op/4,
    transaction_commit/1,
    transaction_get_committed_version/1,
    transaction_get_versionstamp/2,
    transaction_watch/3,
    transaction_on_error/2,
    transaction_reset/1,
    transaction_cancel/1,
    transaction_add_conflict_range/4,
    transaction_get_next_tx_id/1,
    transaction_is_read_only/1,
    transaction_has_watches/1,
    transaction_get_writes_allowed/1,
    transaction_get_approximate_size/1,

    get_error/1,
    error_predicate/2
]).

-export_type([
    atomic_mode/0,
    atomic_operand/0,
    database/0,
    database_option/0,
    error/0,
    error_predicate/0,
    future/0,
    future_result/0,
    key/0,
    key_selector/0,
    tenant/0,
    transaction/0,
    transaction_option/0,
    value/0,
    version/0
]).

-type error()       :: {erlfdb_error, Code :: integer()}.
-type future()      :: {erlfdb_future, pid(), reference()}.
-type database()    :: {erlfdb_database, pid(), reference()}.
-type tenant()      :: {erlfdb_tenant, pid(), reference()}.
-type transaction() :: {erlfdb_transaction, pid(), reference()}.

-type key()     :: binary().
-type value()   :: binary().
-type version() :: integer().

-type compares()     :: lt | lteq | gt | gteq.
-type or_equal()     :: boolean() | 0 | 1.
-type key_selector() ::
    {key(), compares() | or_equal()}
    | {key(), compares() | or_equal(), integer()}.

-type future_result() ::
    database()
    | integer()
    | value()
    | {[{key(), value()}], integer(), boolean()}
    | not_found
    | []
    | [{key(), value()}]
    | [{{key(), value()}, {key(), key()}, list({key(), value()})}]
    | ok.

-type database_option() ::
    location_cache_size | max_watches | machine_id | datacenter_id.

-type transaction_option() ::
    causal_write_risky | causal_read_risky | causal_read_disable
    | next_write_no_write_conflict_range | read_your_writes_disable
    | read_ahead_disable | durability_datacenter | durability_risky
    | durability_dev_null_is_web_scale | priority_system_immediate
    | priority_batch | initialize_new_database | access_system_keys
    | read_system_keys | debug_retry_logging | transaction_logging_enable
    | timeout | retry_limit | max_retry_delay | snapshot_ryw_enable
    | snapshot_ryw_disable | lock_aware | used_during_commit_protection_disable
    | read_lock_aware | size_limit | allow_writes | disallow_writes.

-type atomic_mode() ::
    add | bit_and | bit_or | bit_xor | append_if_fits
    | max | min | byte_min | byte_max
    | set_versionstamped_key | set_versionstamped_value.

-type atomic_operand() :: integer() | binary().

-type error_predicate() :: retryable | maybe_committed | retryable_not_committed.

-type option_value() :: integer() | binary().

%% ---------------------------------------------------------------------------
%% Versioning
%% ---------------------------------------------------------------------------

-spec get_default_api_version() -> integer().
get_default_api_version() -> ?DEFAULT_API_VERSION.

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
%% Future operations
%% ---------------------------------------------------------------------------

-spec future_cancel(future()) -> ok.
future_cancel({erlfdb_future, Worker, FutRef}) ->
    call_ok(Worker, future_cancel, {FutRef}).

-spec future_silence(future()) -> ok.
future_silence({erlfdb_future, Worker, FutRef}) ->
    call_ok(Worker, future_silence, {FutRef}).

-spec future_is_ready(future()) -> boolean().
future_is_ready({erlfdb_future, Worker, FutRef}) ->
    int_to_bool(call_value(Worker, future_is_ready, {FutRef})).

-spec future_get_error(future()) -> ok | error().
future_get_error({erlfdb_future, Worker, FutRef}) ->
    case gen_server:call(Worker, {request, future_get_error, {FutRef}}) of
        ok -> ok;
        {error, Code} when is_integer(Code) -> {erlfdb_error, Code};
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

-spec future_get(future()) -> future_result().
future_get({erlfdb_future, Worker, FutRef}) ->
    case gen_server:call(Worker, {request, future_get, {FutRef}}) of
        {ok, Value}  -> Value;
        not_found    -> not_found;
        ok           -> ok;
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

%% Block until a future is ready, then return its result.
-spec wait(future()) -> future_result().
wait({erlfdb_future, _Worker, FutRef} = Future) ->
    receive
        {FutRef, ready}           -> future_get(Future);
        {{_TxRef, FutRef}, ready} -> future_get(Future)
    end.

%% ---------------------------------------------------------------------------
%% Database operations
%% ---------------------------------------------------------------------------

-spec create_database(ClusterFile :: binary()) -> database().
create_database(ClusterFile) ->
    ok = ensure_started(),
    Worker = erlfdb_worker_sup:pick_worker(),
    DbRef = make_ref(),
    case gen_server:call(Worker, {request, create_database, {DbRef, ClusterFile}}) of
        ok ->
            {erlfdb_database, Worker, DbRef};
        {error, Code} when is_integer(Code) ->
            erlang:error({erlfdb_error, Code});
        {error, Reason} ->
            erlang:error({erlfdb_error, Reason})
    end.

-spec database_open_tenant(database(), TenantName :: binary()) -> tenant().
database_open_tenant({erlfdb_database, Worker, DbRef}, TenantName) ->
    TenantRef = make_ref(),
    case gen_server:call(Worker,
                         {request, database_open_tenant, {DbRef, TenantRef, TenantName}}) of
        ok ->
            {erlfdb_tenant, Worker, TenantRef};
        {error, Code} when is_integer(Code) ->
            erlang:error({erlfdb_error, Code});
        {error, Reason} ->
            erlang:error({erlfdb_error, Reason})
    end.

-spec database_set_option(database(), Option :: database_option()) -> ok.
database_set_option(Db, Option) ->
    database_set_option(Db, Option, <<>>).

-spec database_set_option(database(), Option :: database_option(),
                           Value :: option_value()) -> ok.
database_set_option({erlfdb_database, Worker, DbRef}, Opt, Val) ->
    BinVal = opt_val_to_binary(Val),
    call_ok(Worker, database_set_option, {DbRef, Opt, BinVal}).

-spec database_create_transaction(database()) -> transaction().
database_create_transaction({erlfdb_database, Worker, DbRef}) ->
    TxRef = make_ref(),
    case gen_server:call(Worker,
                         {request, database_create_transaction, {DbRef, TxRef}}) of
        ok ->
            {erlfdb_transaction, Worker, TxRef};
        {error, Code} when is_integer(Code) ->
            erlang:error({erlfdb_error, Code});
        {error, Reason} ->
            erlang:error({erlfdb_error, Reason})
    end.

-spec database_get_main_thread_busyness(database()) -> float().
database_get_main_thread_busyness({erlfdb_database, Worker, DbRef}) ->
    call_value(Worker, database_get_main_thread_busyness, {DbRef}).

-spec database_get_client_status(database()) -> future().
database_get_client_status({erlfdb_database, Worker, DbRef}) ->
    FutRef = make_ref(),
    Args = {DbRef, FutRef, self()},
    case gen_server:call(Worker, {request, database_get_client_status, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

-spec tenant_create_transaction(tenant()) -> transaction().
tenant_create_transaction({erlfdb_tenant, Worker, TenantRef}) ->
    TxRef = make_ref(),
    case gen_server:call(Worker,
                         {request, tenant_create_transaction, {TenantRef, TxRef}}) of
        ok ->
            {erlfdb_transaction, Worker, TxRef};
        {error, Code} when is_integer(Code) ->
            erlang:error({erlfdb_error, Code});
        {error, Reason} ->
            erlang:error({erlfdb_error, Reason})
    end.

%% ---------------------------------------------------------------------------
%% Sync transaction operations
%% ---------------------------------------------------------------------------

transaction_set_option(Tx, Option) ->
    transaction_set_option(Tx, Option, <<>>).

transaction_set_option({erlfdb_transaction, Worker, TxRef}, Opt, Val) ->
    BinVal = opt_val_to_binary(Val),
    call_ok(Worker, transaction_set_option, {TxRef, Opt, BinVal}).

transaction_set_read_version({erlfdb_transaction, Worker, TxRef}, Version) ->
    call_ok(Worker, transaction_set_read_version, {TxRef, Version}).

transaction_set({erlfdb_transaction, Worker, TxRef}, Key, Val) ->
    call_ok(Worker, transaction_set, {TxRef, Key, Val}).

transaction_clear({erlfdb_transaction, Worker, TxRef}, Key) ->
    call_ok(Worker, transaction_clear, {TxRef, Key}).

transaction_clear_range({erlfdb_transaction, Worker, TxRef}, StartKey, EndKey) ->
    call_ok(Worker, transaction_clear_range, {TxRef, StartKey, EndKey}).

transaction_atomic_op({erlfdb_transaction, Worker, TxRef}, Key, Operand, OpName) ->
    BinOperand =
        case Operand of
            Bin when is_binary(Bin) -> Bin;
            Int when is_integer(Int) -> <<Int:64/little>>
        end,
    call_ok(Worker, transaction_atomic_op, {TxRef, Key, BinOperand, OpName}).

transaction_reset({erlfdb_transaction, Worker, TxRef}) ->
    call_ok(Worker, transaction_reset, {TxRef}).

transaction_cancel({erlfdb_transaction, Worker, TxRef}) ->
    call_ok(Worker, transaction_cancel, {TxRef}).

transaction_add_conflict_range({erlfdb_transaction, Worker, TxRef}, StartKey, EndKey, Type) ->
    call_ok(Worker, transaction_add_conflict_range, {TxRef, StartKey, EndKey, Type}).

transaction_get_committed_version({erlfdb_transaction, Worker, TxRef}) ->
    call_value(Worker, transaction_get_committed_version, {TxRef}).

transaction_get_next_tx_id({erlfdb_transaction, Worker, TxRef}) ->
    call_value(Worker, transaction_get_next_tx_id, {TxRef}).

transaction_is_read_only({erlfdb_transaction, Worker, TxRef}) ->
    int_to_bool(call_value(Worker, transaction_is_read_only, {TxRef})).

transaction_has_watches({erlfdb_transaction, Worker, TxRef}) ->
    int_to_bool(call_value(Worker, transaction_has_watches, {TxRef})).

transaction_get_writes_allowed({erlfdb_transaction, Worker, TxRef}) ->
    int_to_bool(call_value(Worker, transaction_get_writes_allowed, {TxRef})).

%% ---------------------------------------------------------------------------
%% Async transaction operations (return futures)
%% ---------------------------------------------------------------------------

transaction_get_read_version({erlfdb_transaction, Worker, TxRef}) ->
    FutRef = make_ref(),
    Args = {TxRef, FutRef, self()},
    case gen_server:call(Worker, {request, transaction_get_read_version, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_get({erlfdb_transaction, Worker, TxRef}, Key, Snapshot) ->
    FutRef = make_ref(),
    Args = {TxRef, Key, Snapshot, FutRef, self()},
    case gen_server:call(Worker, {request, transaction_get, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_get_estimated_range_size({erlfdb_transaction, Worker, TxRef},
                                      StartKey, EndKey) ->
    FutRef = make_ref(),
    Args = {TxRef, StartKey, EndKey, FutRef, self()},
    case gen_server:call(Worker,
                         {request, transaction_get_estimated_range_size, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_get_key({erlfdb_transaction, Worker, TxRef}, KeySelector, Snapshot) ->
    FutRef = make_ref(),
    Args = {TxRef, KeySelector, Snapshot, FutRef, self()},
    case gen_server:call(Worker, {request, transaction_get_key, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_get_addresses_for_key({erlfdb_transaction, Worker, TxRef}, Key) ->
    FutRef = make_ref(),
    Args = {TxRef, Key, FutRef, self()},
    case gen_server:call(Worker,
                         {request, transaction_get_addresses_for_key, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_get_range({erlfdb_transaction, Worker, TxRef},
                       StartKS, EndKS, Limit, TargetBytes,
                       StreamingMode, Iteration, Snapshot, Reverse) ->
    FutRef = make_ref(),
    Args = {TxRef, StartKS, EndKS, Limit, TargetBytes,
            StreamingMode, Iteration, Snapshot, Reverse, FutRef, self()},
    case gen_server:call(Worker, {request, transaction_get_range, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_get_range_split_points({erlfdb_transaction, Worker, TxRef},
                                    StartKey, EndKey, ChunkSize) ->
    FutRef = make_ref(),
    Args = {TxRef, StartKey, EndKey, ChunkSize, FutRef, self()},
    case gen_server:call(Worker,
                         {request, transaction_get_range_split_points, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_get_mapped_range({erlfdb_transaction, Worker, TxRef},
                              StartKS, EndKS, Mapper, Limit, TargetBytes,
                              StreamingMode, Iteration, Snapshot, Reverse) ->
    FutRef = make_ref(),
    Args = {TxRef, StartKS, EndKS, Mapper, Limit, TargetBytes,
            StreamingMode, Iteration, Snapshot, Reverse, FutRef, self()},
    case gen_server:call(Worker, {request, transaction_get_mapped_range, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_commit({erlfdb_transaction, Worker, TxRef}) ->
    FutRef = make_ref(),
    Args = {TxRef, FutRef, self()},
    case gen_server:call(Worker, {request, transaction_commit, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

%% get_versionstamp fires after commit; To is the destination pid.
transaction_get_versionstamp({erlfdb_transaction, Worker, TxRef}, To) ->
    FutRef = make_ref(),
    Args = {TxRef, FutRef, To},
    case gen_server:call(Worker, {request, transaction_get_versionstamp, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

%% watch fires when the key changes; To is the destination pid.
transaction_watch({erlfdb_transaction, Worker, TxRef}, Key, To) ->
    FutRef = make_ref(),
    Args = {TxRef, Key, FutRef, To},
    case gen_server:call(Worker, {request, transaction_watch, Args}) of
        ok                              -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, writes_not_allowed}     -> erlang:error(writes_not_allowed);
        {error, _}                      -> erlang:error(badarg)
    end.

transaction_on_error({erlfdb_transaction, Worker, TxRef}, ErrorCode) ->
    FutRef = make_ref(),
    Args = {TxRef, ErrorCode, FutRef, self()},
    case gen_server:call(Worker, {request, transaction_on_error, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

transaction_get_approximate_size({erlfdb_transaction, Worker, TxRef}) ->
    FutRef = make_ref(),
    Args = {TxRef, FutRef, self()},
    case gen_server:call(Worker,
                         {request, transaction_get_approximate_size, Args}) of
        ok -> {erlfdb_future, Worker, FutRef};
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

%% ---------------------------------------------------------------------------
%% Misc
%% ---------------------------------------------------------------------------

-spec get_error(integer()) -> binary().
get_error(ErrorCode) ->
    ok = ensure_started(),
    Worker = erlfdb_worker_sup:pick_worker(),
    call_value(Worker, get_error, {ErrorCode}).

-spec error_predicate(error_predicate(), integer()) -> boolean().
error_predicate(Predicate, ErrorCode) ->
    ok = ensure_started(),
    Worker = erlfdb_worker_sup:pick_worker(),
    int_to_bool(call_value(Worker, error_predicate, {Predicate, ErrorCode})).

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

call_ok(Worker, Op, Args) ->
    case gen_server:call(Worker, {request, Op, Args}) of
        ok                              -> ok;
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, writes_not_allowed}     -> erlang:error(writes_not_allowed);
        {error, _}                      -> erlang:error(badarg)
    end.

call_value(Worker, Op, Args) ->
    case gen_server:call(Worker, {request, Op, Args}) of
        {ok, Value}                     -> Value;
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, writes_not_allowed}     -> erlang:error(writes_not_allowed);
        {error, _}                      -> erlang:error(badarg)
    end.

int_to_bool(1) -> true;
int_to_bool(0) -> false.

opt_val_to_binary(Val) when is_binary(Val)  -> Val;
opt_val_to_binary(Val) when is_integer(Val) -> <<Val:64/little>>.

ensure_started() ->
    case application:ensure_started(erlfdb) of
        ok -> ok;
        {error, Reason} -> erlang:error({erlfdb_start_failed, Reason})
    end.
