#!/usr/bin/env escript
%% -*- erlang -*-
%%
%% erlfdb benchmark runner.
%%
%% Compares NIF and port implementations for each operation, printing
%% avg/p50/p95/p99/max latency (microseconds) and the overhead ratio.
%%
%% Usage:
%%   rebar3 as test compile
%%   ERL_LIBS=_build/test/lib/erlfdb escript bench/run.escript [iterations]
%%
%% The optional first argument overrides the default iteration count (10 000).
%%
%% Adding a new benchmark:
%%   1. Add an entry to the list returned by benchmarks/1 (which receives the
%%      open Db handle so lambdas can close over it).
%%   2. The entry shape is {Label :: string(), NifFun :: fun(), PortFun :: fun()}.
%%   3. Both funs must be 0-arity; wrap any arguments in the closure.

-mode(compile).

-define(DEFAULT_N, 10000).
-define(SEED_KEY_COUNT, 1000).

main(Args) ->
    N = parse_n(Args),
    io:format("~n=== erlfdb benchmark suite ===~n"),
    io:format("Iterations per benchmark: ~p~n~n", [N]),

    %% Start the erlfdb application (boots the port worker pool).
    {ok, _} = application:ensure_all_started(erlfdb),

    %% Create a sandbox DB and seed it with test data.  The Db handle is
    %% passed to benchmarks/1 so future lambdas can close over it.
    Db = setup_db(),

    %% Run every registered benchmark.
    lists:foreach(
        fun({Label, NifFun, PortFun}) ->
            io:format("--- ~s ---~n", [Label]),
            erlfdb_bench:compare(NifFun, "nif", PortFun, "port", N),
            io:format("~n")
        end,
        benchmarks(Db)
    ),

    halt(0).

%% ---------------------------------------------------------------------------
%% Benchmark registry
%% Add new {Label, NifFun, PortFun} tuples here as operations are ported.
%% ---------------------------------------------------------------------------

benchmarks(_Db) ->
    [
        {
            "get_max_api_version",
            fun erlfdb_nif:get_max_api_version/0,
            fun erlfdb_port:get_max_api_version/0
        }

        %% Future examples (uncomment when the port ops are implemented):
        %%
        %% {
        %%     "create_transaction",
        %%     fun() -> erlfdb_nif:database_create_transaction(Db) end,
        %%     fun() -> erlfdb_port:database_create_transaction(Db) end
        %% },
        %% {
        %%     "get (hot key, snapshot)",
        %%     fun() ->
        %%         erlfdb:transactional(Db, fun(Tx) ->
        %%             erlfdb:wait(erlfdb:get(erlfdb:snapshot(Tx), bench_key(1)))
        %%         end)
        %%     end,
        %%     fun() ->
        %%         erlfdb_port:transactional(Db, fun(Tx) ->
        %%             erlfdb_port:wait(erlfdb_port:get(erlfdb_port:snapshot(Tx), bench_key(1)))
        %%         end)
        %%     end
        %% }
    ].

%% ---------------------------------------------------------------------------
%% DB setup
%% ---------------------------------------------------------------------------

setup_db() ->
    io:format("Starting sandbox fdbserver...~n"),
    Db = erlfdb_sandbox:open(<<"bench">>),
    io:format("Seeding ~p keys...~n", [?SEED_KEY_COUNT]),
    erlfdb:transactional(Db, fun(Tx) ->
        lists:foreach(
            fun(I) ->
                erlfdb:set(Tx, bench_key(I), bench_val(I))
            end,
            lists:seq(1, ?SEED_KEY_COUNT)
        )
    end),
    io:format("DB ready.~n~n"),
    Db.

bench_key(I) ->
    iolist_to_binary(io_lib:format("bench_key_~6..0b", [I])).

bench_val(I) ->
    iolist_to_binary(io_lib:format("bench_val_~6..0b", [I])).

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
