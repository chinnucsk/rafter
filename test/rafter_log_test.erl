-module(rafter_log_test).

-include_lib("eunit/include/eunit.hrl").
-include("rafter.hrl").

-compile(export_all).

setup_test() ->
    rafter_log:start_link('test_log').

entry_to_binary_test() ->
    lists:foreach(fun(Type) ->
        E = #rafter_entry{type=Type, term=1, cmd="blahahbalhfaldjhf"},
        ?assertEqual(E, rafter_log:binary_to_entry(rafter_log:entry_to_binary(E)))
    end, [config, op, vote]).


