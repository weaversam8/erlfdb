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

-module(erlfdb_05_get_next_tx_id_test).

-include_lib("eunit/include/eunit.hrl").

-define(BACKENDS, [erlfdb_nif, erlfdb_port]).

backends_test_() ->
    Tests = [fun t_get_tx_id/0],
    [{atom_to_list(B),
      {setup,
       fun() -> application:set_env(erlfdb, backend, B) end,
       fun(_) -> application:set_env(erlfdb, backend, erlfdb_port) end,
       Tests}}
     || B <- ?BACKENDS].

t_get_tx_id() ->
    Db = erlfdb_sandbox:open(),
    erlfdb:transactional(Db, fun(Tx) ->
        lists:foreach(
            fun(I) ->
                ?assertEqual(I, erlfdb:get_next_tx_id(Tx))
            end,
            lists:seq(0, 65535)
        ),
        ?assertError(badarg, erlfdb:get_next_tx_id(Tx))
    end).
