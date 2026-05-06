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
    get_max_api_version/0,
    create_database/1,
    database_create_transaction/1,
    transaction_set_option/2,
    transaction_set_option/3,
    transaction_set_read_version/2,
    transaction_set/3,
    transaction_clear/2,
    transaction_clear_range/3,
    transaction_atomic_op/4,
    transaction_reset/1,
    transaction_cancel/1,
    transaction_add_conflict_range/4,
    transaction_get_committed_version/1,
    transaction_get_next_tx_id/1,
    transaction_is_read_only/1,
    transaction_has_watches/1,
    transaction_get_writes_allowed/1
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

-spec create_database(ClusterFile :: binary()) ->
    {erlfdb_database, pid(), reference()}.
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

-spec database_create_transaction({erlfdb_database, pid(), reference()}) ->
    {erlfdb_transaction, pid(), reference()}.
database_create_transaction({erlfdb_database, Worker, DbRef}) ->
    TxRef = make_ref(),
    case gen_server:call(Worker, {request, database_create_transaction, {DbRef, TxRef}}) of
        ok ->
            {erlfdb_transaction, Worker, TxRef};
        {error, Code} when is_integer(Code) ->
            erlang:error({erlfdb_error, Code});
        {error, Reason} ->
            erlang:error({erlfdb_error, Reason})
    end.

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
%% Internal helpers
%% ---------------------------------------------------------------------------

%% Send a request to a worker and expect an `ok` reply.
call_ok(Worker, Op, Args) ->
    case gen_server:call(Worker, {request, Op, Args}) of
        ok -> ok;
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

%% Send a request to a worker and expect an `{ok, Value}` reply.
call_value(Worker, Op, Args) ->
    case gen_server:call(Worker, {request, Op, Args}) of
        {ok, Value} -> Value;
        {error, Code} when is_integer(Code) -> erlang:error({erlfdb_error, Code});
        {error, Reason} -> erlang:error({erlfdb_error, Reason})
    end.

%% Convert the 0/1 integers the C side uses for boolean flags to atoms.
int_to_bool(1) -> true;
int_to_bool(0) -> false.

%% Normalise option values to binary, mirroring erlfdb_nif:option_val_to_binary/1.
opt_val_to_binary(Val) when is_binary(Val) -> Val;
opt_val_to_binary(Val) when is_integer(Val) -> <<Val:64/little>>.

ensure_started() ->
    case application:ensure_started(erlfdb) of
        ok -> ok;
        {error, Reason} -> erlang:error({erlfdb_start_failed, Reason})
    end.
