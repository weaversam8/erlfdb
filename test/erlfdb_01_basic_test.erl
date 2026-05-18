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

-module(erlfdb_01_basic_test).

-include_lib("eunit/include/eunit.hrl").

-define(BACKENDS, [erlfdb_nif, erlfdb_port]).

backends_test_() ->
    [{atom_to_list(B),
      {setup,
       fun() -> application:set_env(erlfdb, backend, B) end,
       fun(_) -> application:set_env(erlfdb, backend, erlfdb_port) end,
       [fun t_load/0, fun t_get_error_string/0]}}
     || B <- ?BACKENDS].

t_load() ->
    ok = application:ensure_started(erlfdb),
    ok.

t_get_error_string() ->
    ok = application:ensure_started(erlfdb),
    ?assertEqual(<<"Success">>, erlfdb:get_error_string(0)),
    ?assertEqual(
        <<"Transaction exceeds byte limit">>,
        erlfdb:get_error_string(2101)
    ),
    ?assertEqual(<<"UNKNOWN_ERROR">>, erlfdb:get_error_string(9999)).
