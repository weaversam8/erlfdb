#!/usr/bin/env escript
%% -*- erlang -*-
%%
%% erlfdb benchmark runner.
%%
%% Compares NIF and port implementations for each operation, printing
%% avg/p50/p95/p99/max latency (microseconds) and the overhead ratio.
%%
%% Usage:
%%   rebar3 compile
%%   ERL_LIBS=_build/default/lib/erlfdb escript bench/run.escript [iterations]
%%
%% The optional first argument overrides the default iteration count (10 000).
%% Individual benchmarks can set a lower cap (see benchmarks/1 below).
%%
%% Adding a new benchmark:
%%   Add a tuple to the list returned by benchmarks/1. The map carries
%%   nif_db, port_db, nif_tx, port_tx, and cluster_file.
%%
%%   Tuple shapes:
%%     {Label, NifFun, PortFun}           -- uses the global N
%%     {Label, NifFun, PortFun, MaxN}     -- caps at MaxN regardless of global N
%%
%% Backend selection:
%%   erlfdb:* functions dispatch to erlfdb_nif or erlfdb_port based on the
%%   `backend` application env key. Both handles are created in setup_db/0 so
%%   the benchmark lambdas can use the same erlfdb:* calls with different
%%   handles without touching the env var at call time.

-mode(compile).

-define(DEFAULT_N, 10000).
-define(SEED_KEY_COUNT, 1000).

main(Args) ->
    N = parse_n(Args),
    io:format("~n=== erlfdb benchmark suite ===~n"),
    io:format("Default iterations: ~p~n~n", [N]),

    %% Start the erlfdb application (boots the port worker pool).
    {ok, _} = application:ensure_all_started(erlfdb),

    %% Create a sandbox DB and seed it with test data.
    DbMap = setup_db(),
    run_all(DbMap, N),
    halt(0).

%% ---------------------------------------------------------------------------
%% Benchmark registry
%%
%% Each entry: {Label, NifFun, PortFun} or {Label, NifFun, PortFun, MaxN}.
%% ---------------------------------------------------------------------------

benchmarks(#{
    nif_db := NifDb,
    port_db := PortDb,
    nif_tx := NifTx,
    port_tx := PortTx,
    cluster_file := ClusterFile
}) ->
    [
        %% ----- Stateless / no-resource ops -----
        {
            "get_max_api_version",
            fun erlfdb_nif:get_max_api_version/0,
            fun erlfdb_port:get_max_api_version/0
        },
        {
            "get_error (code -> string)",
            fun() -> erlfdb_nif:get_error(1007) end,
            fun() -> erlfdb_port:get_error(1007) end
        },
        {
            "error_predicate (retryable, 1007)",
            fun() -> erlfdb_nif:error_predicate(retryable, 1007) end,
            fun() -> erlfdb_port:error_predicate(retryable, 1007) end
        },

        %% ----- Resource creation -----
        {
            "create_database",
            fun() -> erlfdb_nif:create_database(ClusterFile) end,
            fun() -> erlfdb_port:create_database(ClusterFile) end,
            %% Each call opens an FDB client connection; keep iterations low.
            500
        },
        {
            "database_create_transaction",
            fun() -> erlfdb_nif:database_create_transaction(NifDb) end,
            fun() -> erlfdb_port:database_create_transaction(PortDb) end
        },
        {
            "database_get_main_thread_busyness",
            fun() -> erlfdb_nif:database_get_main_thread_busyness(NifDb) end,
            fun() -> erlfdb_port:database_get_main_thread_busyness(PortDb) end
        },

        %% ----- Sync transaction ops (local, no network roundtrip) -----
        %% Both sides use pre-created transactions that are never committed,
        %% so the FDB server is never contacted.  Isolates IPC overhead (port)
        %% vs in-process call overhead (NIF).

        {
            "transaction_set",
            fun() -> erlfdb:set(NifTx, <<"bench_key">>, <<"v">>) end,
            fun() -> erlfdb:set(PortTx, <<"bench_key">>, <<"v">>) end
        },
        {
            "transaction_clear",
            fun() -> erlfdb:clear(NifTx, <<"bench_key">>) end,
            fun() -> erlfdb:clear(PortTx, <<"bench_key">>) end
        },
        {
            "transaction_atomic_op (add)",
            fun() -> erlfdb:add(NifTx, <<"counter">>, 1) end,
            fun() -> erlfdb:add(PortTx, <<"counter">>, 1) end
        },
        {
            "transaction_is_read_only",
            fun() -> erlfdb:is_read_only(NifTx) end,
            fun() -> erlfdb:is_read_only(PortTx) end
        },
        {
            "transaction_has_watches",
            fun() -> erlfdb:has_watches(NifTx) end,
            fun() -> erlfdb:has_watches(PortTx) end
        },
        {
            "transaction_get_writes_allowed",
            fun() -> erlfdb:get_writes_allowed(NifTx) end,
            fun() -> erlfdb:get_writes_allowed(PortTx) end
        },
        {
            "transaction_get_next_tx_id",
            fun() -> erlfdb:get_next_tx_id(NifTx) end,
            fun() -> erlfdb:get_next_tx_id(PortTx) end
        },
        {
            %% Reset clears accumulated mutations; fine to run in a tight loop.
            "transaction_reset",
            fun() -> erlfdb:reset(NifTx) end,
            fun() -> erlfdb:reset(PortTx) end
        },

        %% ----- Async ops with real FDB network roundtrips -----
        %% Each benchmark creates a fresh transaction per iteration so numbers
        %% include create_transaction overhead (~1 us NIF / ~13 us port).
        %% The dominant cost is FDB network latency.

        {
            "transaction_commit (set one key + commit)",
            fun() ->
                Tx = erlfdb:create_transaction(NifDb),
                erlfdb:set(Tx, bench_key(1), bench_val(1)),
                erlfdb:wait(erlfdb:commit(Tx))
            end,
            fun() ->
                Tx = erlfdb:create_transaction(PortDb),
                erlfdb:set(Tx, bench_key(1), bench_val(1)),
                erlfdb:wait(erlfdb:commit(Tx))
            end
        },
        {
            "transaction_get (snapshot read, hot key)",
            fun() ->
                Tx = erlfdb:create_transaction(NifDb),
                erlfdb:wait(erlfdb:get(erlfdb:snapshot(Tx), bench_key(1)))
            end,
            fun() ->
                Tx = erlfdb:create_transaction(PortDb),
                erlfdb:wait(erlfdb:get(erlfdb:snapshot(Tx), bench_key(1)))
            end
        },
        {
            "transaction_get_read_version",
            fun() ->
                Tx = erlfdb:create_transaction(NifDb),
                erlfdb:wait(erlfdb:get_read_version(Tx))
            end,
            fun() ->
                Tx = erlfdb:create_transaction(PortDb),
                erlfdb:wait(erlfdb:get_read_version(Tx))
            end
        },
        {
            "transaction_get_range (10 keys, snapshot)",
            fun() ->
                Tx = erlfdb:create_transaction(NifDb),
                SS = erlfdb:snapshot(Tx),
                erlfdb:wait(erlfdb:get_range(SS, bench_key(1), bench_key(11)))
            end,
            fun() ->
                Tx = erlfdb:create_transaction(PortDb),
                SS = erlfdb:snapshot(Tx),
                erlfdb:wait(erlfdb:get_range(SS, bench_key(1), bench_key(11)))
            end
        },
        {
            "transaction_get_estimated_range_size (100 keys)",
            fun() ->
                Tx = erlfdb:create_transaction(NifDb),
                erlfdb:wait(erlfdb:get_estimated_range_size(Tx, bench_key(1), bench_key(101)))
            end,
            fun() ->
                Tx = erlfdb:create_transaction(PortDb),
                erlfdb:wait(erlfdb:get_estimated_range_size(Tx, bench_key(1), bench_key(101)))
            end
        },
        {
            %% Local computation — no FDB network contact after create_transaction.
            %% Measures the async-future round-trip cost for a result computed
            %% in-process by FDB (write set size).
            "transaction_get_approximate_size (after set)",
            fun() ->
                Tx = erlfdb:create_transaction(NifDb),
                erlfdb:set(Tx, bench_key(1), bench_val(1)),
                erlfdb:wait(erlfdb:get_approximate_size(Tx))
            end,
            fun() ->
                Tx = erlfdb:create_transaction(PortDb),
                erlfdb:set(Tx, bench_key(1), bench_val(1)),
                erlfdb:wait(erlfdb:get_approximate_size(Tx))
            end
        },
        {
            "transaction_get_addresses_for_key",
            fun() ->
                Tx = erlfdb:create_transaction(NifDb),
                erlfdb:wait(erlfdb:get_addresses_for_key(Tx, bench_key(1)))
            end,
            fun() ->
                Tx = erlfdb:create_transaction(PortDb),
                erlfdb:wait(erlfdb:get_addresses_for_key(Tx, bench_key(1)))
            end
        }
    ].

%% ---------------------------------------------------------------------------
%% Runner
%% ---------------------------------------------------------------------------

run_all(DbMap, N) ->
    lists:foreach(
        fun(Entry) ->
            {Label, NifFun, PortFun, BenchN} =
                case Entry of
                    {L, F1, F2} -> {L, F1, F2, N};
                    {L, F1, F2, Max} -> {L, F1, F2, min(N, Max)}
                end,
            io:format("--- ~s ---~n", [Label]),
            erlfdb_bench:compare(NifFun, "nif", PortFun, "port", BenchN),
            io:format("~n")
        end,
        benchmarks(DbMap)
    ).

%% ---------------------------------------------------------------------------
%% DB setup
%% ---------------------------------------------------------------------------

setup_db() ->
    io:format("Starting sandbox fdbserver...~n"),
    Options = erlfdb_sandbox:default_options(),
    {ok, ClusterFile} = erlfdb_util:init_test_cluster(Options),

    %% Create a NIF-backed handle by setting the backend env first.
    %% erlfdb:* dispatch is then determined by the handle's tuple shape at
    %% call time, so no further env-var flipping is needed during benchmarks.
    application:set_env(erlfdb, backend, erlfdb_nif),
    NifDb = erlfdb:open(ClusterFile),
    NifTx = erlfdb:create_transaction(NifDb),

    application:set_env(erlfdb, backend, erlfdb_port),
    PortDb = erlfdb:open(ClusterFile),
    PortTx = erlfdb:create_transaction(PortDb),

    io:format("Seeding ~p keys...~n", [?SEED_KEY_COUNT]),
    erlfdb:transactional(PortDb, fun(Tx) ->
        lists:foreach(
            fun(I) -> erlfdb:set(Tx, bench_key(I), bench_val(I)) end,
            lists:seq(1, ?SEED_KEY_COUNT)
        )
    end),
    io:format("DB ready.~n~n"),
    #{
        nif_db       => NifDb,
        port_db      => PortDb,
        nif_tx       => NifTx,
        port_tx      => PortTx,
        cluster_file => ClusterFile
    }.

bench_key(I) -> iolist_to_binary(io_lib:format("bench_key_~6..0b", [I])).
bench_val(I) -> iolist_to_binary(io_lib:format("bench_val_~6..0b", [I])).

%% ---------------------------------------------------------------------------
%% Argument parsing
%% ---------------------------------------------------------------------------

parse_n([Arg | _]) ->
    try
        N = list_to_integer(Arg),
        if N > 0 -> N; true -> ?DEFAULT_N end
    catch
        _:_ -> ?DEFAULT_N
    end;
parse_n([]) ->
    ?DEFAULT_N.
