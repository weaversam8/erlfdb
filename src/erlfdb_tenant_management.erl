-module(erlfdb_tenant_management).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Managing [Tenants](https://apple.github.io/foundationdb/tenants.html)

> #### Warning {: .warning}
>
> Tenants are currently experimental and are not recommended for use in production.
""".
-endif.

-export([
    transactional/2, list_tenants/1, list_tenants/4, get_tenant/2, create_tenant/2, delete_tenant/2
]).

-define(TENANT_MAP(TenantName),
    iolist_to_binary([<<16#FF, 16#FF, "/management/tenant/map/">>, TenantName])
).

-define(IS_DB,     {erlfdb_database, _, _}).
-define(IS_NIF_DB, {erlfdb_database, _}).
-define(IS_TX,     {erlfdb_transaction, _, _}).
-define(IS_NIF_TX, {erlfdb_transaction, _}).

-spec transactional(erlfdb:database(), function()) -> any().
transactional(?IS_DB = Db, UserFun) ->
    erlfdb:transactional(Db, fun(Tx) ->
        ok = erlfdb:set_option(Tx, special_key_space_enable_writes),
        UserFun(Tx)
    end);
transactional(?IS_NIF_DB = Db, UserFun) ->
    erlfdb:transactional(Db, fun(Tx) ->
        ok = erlfdb:set_option(Tx, special_key_space_enable_writes),
        UserFun(Tx)
    end).

-spec list_tenants(erlfdb:database() | erlfdb:transaction()) -> [erlfdb:tenant_name()].
list_tenants(DbOrTx) ->
    % Tenant names cannot start with \xff, so this is guaranteed to get all tenants
    list_tenants(DbOrTx, <<>>, <<16#FF>>, []).

-spec list_tenants(erlfdb:database() | erlfdb:transaction(), erlfdb:key(), erlfdb:key(), [
    erlfdb:fold_option()
]) -> [erlfdb:tenant_name()].
list_tenants(?IS_DB = Db, Begin, End, Opts) ->
    erlfdb:transactional(Db, fun(Tx) ->
        ok = erlfdb:set_option(Tx, read_system_keys),
        erlfdb:wait(list_tenants(Tx, Begin, End, Opts))
    end);
list_tenants(?IS_NIF_DB = Db, Begin, End, Opts) ->
    erlfdb:transactional(Db, fun(Tx) ->
        ok = erlfdb:set_option(Tx, read_system_keys),
        erlfdb:wait(list_tenants(Tx, Begin, End, Opts))
    end);
list_tenants(?IS_TX = Tx, Begin, End, Opts) ->
    erlfdb:get_range(Tx, ?TENANT_MAP(Begin), ?TENANT_MAP(End), Opts);
list_tenants(?IS_NIF_TX = Tx, Begin, End, Opts) ->
    erlfdb:get_range(Tx, ?TENANT_MAP(Begin), ?TENANT_MAP(End), Opts).

-spec get_tenant(erlfdb:database() | erlfdb:transaction(), erlfdb:tenant_name()) ->
    erlfdb:future() | erlfdb:result().
get_tenant(?IS_DB = Db, TenantName) ->
    erlfdb:transactional(Db, fun(Tx) ->
        ok = erlfdb:set_option(Tx, read_system_keys),
        erlfdb:wait(get_tenant(Tx, TenantName))
    end);
get_tenant(?IS_NIF_DB = Db, TenantName) ->
    erlfdb:transactional(Db, fun(Tx) ->
        ok = erlfdb:set_option(Tx, read_system_keys),
        erlfdb:wait(get_tenant(Tx, TenantName))
    end);
get_tenant(?IS_TX = Tx, TenantName) ->
    erlfdb:get(Tx, ?TENANT_MAP(TenantName));
get_tenant(?IS_NIF_TX = Tx, TenantName) ->
    erlfdb:get(Tx, ?TENANT_MAP(TenantName)).

-spec create_tenant(erlfdb:database() | erlfdb:transaction(), erlfdb:tenant_name()) -> ok.
create_tenant(?IS_DB = Db, TenantName) ->
    transactional(Db, fun(Tx) ->
        case erlfdb:wait(get_tenant(Tx, TenantName)) of
            not_found ->
                create_tenant(Tx, TenantName);
            _ ->
                % tenant_already_exists
                erlang:error({erlfdb_error, 2132})
        end
    end);
create_tenant(?IS_NIF_DB = Db, TenantName) ->
    transactional(Db, fun(Tx) ->
        case erlfdb:wait(get_tenant(Tx, TenantName)) of
            not_found ->
                create_tenant(Tx, TenantName);
            _ ->
                erlang:error({erlfdb_error, 2132})
        end
    end);
create_tenant(?IS_TX = Tx, TenantName) ->
    erlfdb:set(Tx, ?TENANT_MAP(TenantName), <<>>);
create_tenant(?IS_NIF_TX = Tx, TenantName) ->
    erlfdb:set(Tx, ?TENANT_MAP(TenantName), <<>>).

-spec delete_tenant(erlfdb:database() | erlfdb:transaction(), erlfdb:tenant_name()) -> ok.
delete_tenant(?IS_DB = Db, TenantName) ->
    transactional(Db, fun(Tx) ->
        delete_tenant(Tx, TenantName)
    end);
delete_tenant(?IS_NIF_DB = Db, TenantName) ->
    transactional(Db, fun(Tx) ->
        delete_tenant(Tx, TenantName)
    end);
delete_tenant(?IS_TX = Tx, TenantName) ->
    erlfdb:clear(Tx, ?TENANT_MAP(TenantName));
delete_tenant(?IS_NIF_TX = Tx, TenantName) ->
    erlfdb:clear(Tx, ?TENANT_MAP(TenantName)).
