-module(rafter_log_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(eqc_statem).

%% eqc_statem exports
-export([command/1, initial_state/0, next_state/3, postcondition/3,
         precondition/2]).

-include("rafter.hrl").

-compile(export_all).

-record(state, {log_length :: non_neg_integer()}).

-define(QC_OUT(P),
    eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args) end, P)).

%% ====================================================================
%% Tests
%% ====================================================================

eqc_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 30,
            ?_assertEqual(true, eqc:quickcheck(?QC_OUT(eqc:numtests(100, prop_log()))))}
       ]
      }
     ]
    }.

setup() ->
    ok.

cleanup(_) ->
    ok.


%% ====================================================================
%% eqc_statem callbacks
%% ====================================================================
initial_state() ->
    #state{log_length=0}.

command(_S) ->
    oneof([{call, rafter_log, append, [entries()]},
           {call, rafter_log, get_last_index, []},
           {call, rafter_log, get_last_entry, []},
           {call, rafter_log, truncate, [rafter_gen:non_neg_integer()]},
           {call, rafter_log, get_entry, [rafter_gen:non_neg_integer()]}]).

precondition(_S, _) ->
    true.

next_state(S, _V, {call, rafter_log, append, []}) ->
    S;
next_state(S, _V, {call, rafter_log, append, [Entries]}) ->
    Len = S#state.log_length,
    S#state{log_length=Len+length(Entries)};
next_state(#state{log_length=Len}=S, _, 
    {call, rafter_log, truncate, [Index]}) when Len >= Index ->
        S#state{log_length=Index};
next_state(S, _V, {call, _, _, _}) ->
    S.
    
postcondition(_S, {call, rafter_log, append, [_Entries]}, {ok, _Index}) ->
    true;
postcondition(S, {call, rafter_log, get_last_index, []}, V) ->
    ?assertEqual(S#state.log_length, V),
    true;
postcondition(S, {call, rafter_log, get_last_entry, []}, {ok, not_found}) ->
    ?assertEqual(S#state.log_length,0),
    true;
postcondition(_S, {call, rafter_log, get_last_entry, []}, {ok, _Entry}) ->
    true;
postcondition(_S, {call, rafter_log, get_entry, [0]}, {ok, not_found}) ->
    true;
postcondition(S, {call, rafter_log, get_entry, [Index]}, {ok, not_found}) ->
    ?assert(S#state.log_length < Index),
    true;
postcondition(S, {call, rafter_log, get_entry, [Index]}, {ok, _Entry}) ->
    ?assert(S#state.log_length >= Index),
    true;
postcondition(S, {call, rafter_log, truncate, [Index]}, {error, bad_index}) ->
    ?assert(S#state.log_length < Index),
    true;
postcondition(S, {call, rafter_log, truncate, [Index]}, ok) ->
    ?assert(S#state.log_length >= Index),
    true.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_log() ->
    ?FORALL(Cmds,commands(?MODULE),
        begin 
            {ok, _Pid} = rafter_log:start(),
            {H,_S,Res} = run_commands(?MODULE,Cmds),
            ?WHENFAIL(io:format("history is ~p ~n", [H]), equals(ok, Res)),
            ok = rafter_log:stop(),
            timer:sleep(10),
            Res==ok
        end).

%% ====================================================================
%% EQC Generators 
%% ====================================================================

entry() ->
    #rafter_entry{
        term = rafter_gen:non_neg_integer(),
        cmd = eqc_gen:binary()}.

entries() ->
    list(entry()).

-endif.
