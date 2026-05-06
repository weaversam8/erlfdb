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
%%   Add a tuple to the list returned by benchmarks/1. The map argument
%%   carries nif_db, port_db, and cluster_file so lambdas can close over
%%   whichever handle they need.
%%
%%   Tuple shapes:
%%     {Label, NifFun, PortFun}           -- uses the global N
%%     {Label, NifFun, PortFun, MaxN}     -- caps at MaxN regardless of global N

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
    cluster_file := ClusterFile
}) ->
    [
        {
            "get_max_api_version",
            fun erlfdb_nif:get_max_api_version/0,
            fun erlfdb_port:get_max_api_version/0
        },
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
        }

        %% Future examples (uncomment when the port ops are implemented):
        %%
        %% {
        %%     "get (hot key, snapshot)",
        %%     fun() ->
        %%         erlfdb:transactional(NifDb, fun(Tx) ->
        %%             erlfdb:wait(erlfdb:get(erlfdb:snapshot(Tx), bench_key(1)))
        %%         end)
        %%     end,
        %%     fun() ->
        %%         erlfdb_port:transactional(PortDb, fun(Tx) ->
        %%             erlfdb_port:wait(erlfdb_port:get(erlfdb_port:snapshot(Tx), bench_key(1)))
        %%         end)
        %%     end
        %% }
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

    %% One NIF-backed handle and one port-backed handle from the same server.
    NifDb = erlfdb:open(ClusterFile),
    PortDb = erlfdb_port:create_database(ClusterFile),

    io:format("Seeding ~p keys...~n", [?SEED_KEY_COUNT]),
    erlfdb:transactional(NifDb, fun(Tx) ->
        lists:foreach(
            fun(I) -> erlfdb:set(Tx, bench_key(I), bench_val(I)) end,
            lists:seq(1, ?SEED_KEY_COUNT)
        )
    end),
    io:format("DB ready.~n~n"),
    #{
        nif_db => NifDb,
        port_db => PortDb,
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
