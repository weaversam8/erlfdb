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

%% Micro-benchmark helper for comparing NIF vs port call overhead.
%%
%% Usage from a rebar3 shell or from any EUnit test:
%%
%%   %% Compare a specific function pair
%%   erlfdb_bench:compare(
%%       fun erlfdb_nif:get_max_api_version/0, "nif",
%%       fun erlfdb_port:get_max_api_version/0, "port"
%%   ).
%%
%%   %% Benchmark any 0-arity fun
%%   Stats = erlfdb_bench:bench(fun() -> erlfdb:get(Db, <<"k">>) end, 1000),
%%   erlfdb_bench:print_stats("get", Stats).

-module(erlfdb_bench).

-export([
    bench/2,
    compare/4,
    compare/5,
    print_stats/2
]).

-define(DEFAULT_N, 10000).
-define(WARMUP_N, 3).

%% Run Fun N times and return latency statistics (in microseconds).
-spec bench(fun(() -> any()), pos_integer()) ->
    #{avg := integer(), p50 := integer(), p95 := integer(), p99 := integer(),
      max := integer(), n := integer()}.
bench(Fun, N) when is_function(Fun, 0), is_integer(N), N > 0 ->
    lists:foreach(fun(_) -> Fun() end, lists:seq(1, ?WARMUP_N)),
    Times = [begin {T, _} = timer:tc(Fun), T end || _ <- lists:seq(1, N)],
    stats(Times).

%% Compare two zero-arity funs using the default iteration count.
-spec compare(fun(), string(), fun(), string()) -> {map(), map()}.
compare(Fun1, Label1, Fun2, Label2) ->
    compare(Fun1, Label1, Fun2, Label2, ?DEFAULT_N).

%% Compare two zero-arity funs using N iterations each.
-spec compare(fun(), string(), fun(), string(), pos_integer()) -> {map(), map()}.
compare(Fun1, Label1, Fun2, Label2, N) ->
    S1 = bench(Fun1, N),
    S2 = bench(Fun2, N),
    io:format("~n~s (N=~p):~n", [Label1, N]),
    print_stats(Label1, S1),
    io:format("~n~s (N=~p):~n", [Label2, N]),
    print_stats(Label2, S2),
    Avg1 = maps:get(avg, S1),
    Avg2 = maps:get(avg, S2),
    case Avg1 of
        0 ->
            io:format("~nRatio: ~s is infinitely slower (baseline is sub-microsecond)~n",
                      [Label2]);
        _ ->
            Ratio = Avg2 / Avg1,
            io:format("~nOverhead (~s vs ~s avg): ~.1fx~n", [Label2, Label1, Ratio])
    end,
    {S1, S2}.

%% Print a stats map produced by bench/2.
-spec print_stats(string(), map()) -> ok.
print_stats(Label, #{avg := Avg, p50 := P50, p95 := P95, p99 := P99, max := Max, n := N}) ->
    io:format("  ~s  n=~p  avg=~b us  p50=~b us  p95=~b us  p99=~b us  max=~b us~n",
              [Label, N, Avg, P50, P95, P99, Max]).

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

stats(Times) ->
    Sorted = lists:sort(Times),
    N = length(Sorted),
    #{
        n => N,
        avg => lists:sum(Sorted) div N,
        p50 => lists:nth(max(1, N div 2), Sorted),
        p95 => lists:nth(max(1, (N * 95) div 100), Sorted),
        p99 => lists:nth(max(1, (N * 99) div 100), Sorted),
        max => lists:last(Sorted)
    }.
