-module(rafter_log).

-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").

-include("rafter.hrl").

%% API
-export([start_link/1, append/1, append/2, binary_to_entry/1, entry_to_binary/1,
        get_last_entry/0, get_last_entry/1, get_entry/1, get_entry/2,
        get_term/1, get_term/2, get_last_index/0, get_last_index/1, 
        get_last_term/0, get_last_term/1, truncate/1, truncate/2,
        get_voted_for/1, set_voted_for/2, get_current_term/1, set_current_term/2,
        get_config/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, format_status/2]).

%%============================================================================= 
%% Logfile Structure
%%============================================================================= 
%%@doc A log is made up of entries. Each entry is a binary of arbitrary size. However,
%%     the format of an entry is fixed. It's described below.
%%
%%         Entry Format
%%         ----------------
%%         <<Sha1:20/binary, Type:8, Term:64, DataSize:32, Data/binary>> 
%%    
%%         Sha1 - hash of the rest of the entry,
%%         Type - ?CONFIG | ?OP | ?META
%%         Term - The current raft term
%%         DataSize - The size of Data in bytes
%%         Data - Data encoded with term_to_binary/1
%%    
%%     After each log entry a trailer is written. The trailer is used for 
%%     detecting incomplete/corrupted writes, pointing to metadata and
%%     traversing the log file backwards.
%%
%%         Trailer Format
%%         ----------------
%%         <<Crc:32, MetadataPtr:64, Start:64, <<"\xFE\xED\xFE\xED">> >>
%%
%%         Crc - crc32 of the rest of the trailer
%%         MetadataPtr - file location of the metadata entry
%%         Start - file location of the start of this entry
%%         <<"\xFE\xED\xFE\xED">> - magic number marking the end of the trailer.
%%                                  A fully consistent log should always have
%%                                  this magic number as the last 8 bytes.
%%

-record(state, {
    file :: file:io_device(),
    write_location = 0 :: non_neg_integer(),
    meta_location = 0 :: non_neg_integer(),
    config :: #config{},
    meta :: #meta{},
    index = 0 :: non_neg_integer(),
    term = 0 :: non_neg_integer()}).

-record(meta, {
    voted_for :: peer(),
    voted_for_term :: non_neg_integer(),
    config_location :: non_neg_integer()
    }).

-define(MAGIC, <<"\xFE\xED\xFE\xED">>).
-define(HEADER_SIZE, 33).
-define(TRAILER_SIZE, 28).
-define(READ_BLOCK_SIZE, 1048576). %% 1MB

%% Entry Types
-define(CONFIG, 1).
-define(OP, 2).
-define(META, 3).
-define(ALL, [?CONFIG, ?OP, ?META]).

%%====================================================================
%% API
%%====================================================================
entry_to_binary(#rafter_entry{type=config, term=Term, cmd=Data}) ->
    entry_to_binary(?CONFIG, Term, Data);
entry_to_binary(#rafter_entry{type=op, term=Term, cmd=Data}) ->
    entry_to_binary(?OP, Term, Data);
entry_to_binary(#rafter_entry{type=meta, term=Term, cmd=Data}) ->
    entry_to_binary(?META, Term, Data).

entry_to_binary(Type, Term, Data) ->
    BinData = term_to_binary(Data),
    B0 = <<Type:8, Term:64, (byte_size(BinData)):32, BinData/binary>>,
    Sha1 = crypto:hash(sha, B0),
    <<Sha1/binary, B0/binary>>.

binary_to_entry(<<Sha1:20/binary, Type:8, Term:64, Size:32, Data/binary>>) ->
    %% We want to crash on badmatch here if if our log is corrupt 
    %% TODO: Allow an operator to repair the log by truncating at that point
    %% or repair each entry 1 by 1 by consulting a good log.
    Sha1 = crypto:hash(sha, <<Type:8, Term:64, Size:32, Data/binary>>),
    binary_to_entry(Type, Term, Data).

binary_to_entry(?CONFIG, Term, Data) ->
    #rafter_entry{type=config, term=Term, cmd=binary_to_term(Data)};
binary_to_entry(?OP, Term, Data) ->
    #rafter_entry{type=op, term=Term, cmd=binary_to_term(Data)};
binary_to_entry(?META, Term, Data) ->
    #rafter_entry{type=meta, term=Term, cmd=binary_to_term(Data)}.

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

append(Entries) ->
    gen_server:call(?MODULE, {append, Entries}).

append(Name, Entries) ->
    gen_server:call(Name, {append, Entries}).

get_config(Name) ->
    gen_server:call(Name, get_config).

get_last_index() ->
    gen_server:call(?MODULE, get_last_index).

get_last_index(Name) ->
    gen_server:call(Name, get_last_index).

get_last_entry() ->
    gen_server:call(?MODULE, get_last_entry).

get_last_entry(Name) ->
    gen_server:call(Name, get_last_entry).

get_last_term() ->
    case get_last_entry() of
        {ok, #rafter_entry{term=Term}} ->
            Term;
        {ok, not_found} ->
            0
    end.

get_last_term(Name) ->
    case get_last_entry(Name) of
        {ok, #rafter_entry{term=Term}} ->
            Term;
        {ok, not_found} ->
            0
    end.

set_voted_for(Name, VotedFor) ->
    gen_server:call(Name, {set_voted_for, VotedFor}).

get_voted_for(Name) ->
    gen_server:call(Name, get_voted_for).

set_current_term(Name, CurrentTerm) ->
    gen_server:call(Name, {set_current_term, CurrentTerm}).

get_current_term(Name) ->
    gen_server:call(Name, get_current_term).

get_entry(Index) ->
    gen_server:call(?MODULE, {get_entry, Index}).

get_entry(Name, Index) ->
    gen_server:call(Name, {get_entry, Index}).

get_term(Index) ->
    case get_entry(Index) of
        {ok, #rafter_entry{term=Term}} ->
            Term;
        {ok, not_found} ->
            0
    end.

get_term(Name, Index) ->
    case get_entry(Name, Index) of
        {ok, #rafter_entry{term=Term}} ->
            Term;
        {ok, not_found} ->
            0
    end.

truncate(Index) ->
    gen_server:call(?MODULE, {truncate, Index}).

truncate(Name, Index) ->
    gen_server:call(Name, {truncate, Index}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Name]) ->
    Filename = "rafter_"++atom_to_list(Name)++".log",
    {ok, File} = file:open(Filename, [append, read, binary]),
    {ok, #file_info{size=Size}} = read_file_info(Filename),
    {MetaLocation, Meta, Config, Term} = init_file(File, Size),
    {ok, #state{file=File,
                write_location=Size,
                meta_location=MetaLocation,
                term=Term,
                meta=Meta,
                config=Config}}.

format_status(_, [_, State]) ->
    Data = lager:pr(State, ?MODULE),
    [{data, [{"StateData", Data}]}].

handle_call({append, Entries}, _From, 
            #state{file=File, config=C, index=I, 
                   write_location=Loc, meta_location=MLoc}=State) ->
    Config = find_config(Entries, C),
    {NewLoc, NumWritten} = write_entries(File, Entries, Loc, MLoc),
    Index = I + NumWritten,
    NewState = State#state{index=Index, 
                           config=Config, 
                           write_location=NewLoc},
    {reply, {ok, Index}, NewState};

handle_call(get_config, _From, #state{config=Config}=State) ->
    {reply, Config, State};

handle_call(get_last_entry, _From, #state{entries=[]}=State) ->
    {reply, {ok, not_found}, State};
handle_call(get_last_entry, _From, #state{entries=[H | _T]}=State) ->
    {reply, {ok, H}, State};

handle_call(get_last_index, _From, #state{entries=Entries}=State) ->
    {reply, Index, State};

handle_call({set_voted_for, VotedFor}, _From, State) ->
    {reply, ok, State#state{voted_for=VotedFor}};
handle_call(get_voted_for, _From, #state{voted_for=VotedFor}=State) ->
    {reply, {ok, VotedFor}, State};

handle_call({set_current_term, Term}, _From, State) ->
    {reply, ok, State#state{term=Term}};
handle_call(get_current_term, _From, #state{term=Term}=State) ->
    {reply, {ok, Term}, State};

handle_call({get_entry, Index}, _From, #state{entries=Entries}=State) ->
    Entry = try 
        lists:nth(Index, lists:reverse(Entries))
    catch _:_ ->
        not_found
    end, 
    {reply, {ok, Entry}, State};

handle_call({truncate, 0}, _From, #state{entries=[]}=State) ->
    {reply, ok, State};
handle_call({truncate, Index}, _From, #state{entries=Entries}=State) 
        when Index > length(Entries) ->
    {reply, {error, bad_index}, State};
handle_call({truncate, Index}, _From, #state{entries=Entries}=State) ->
    NewEntries = lists:reverse(lists:sublist(lists:reverse(Entries), Index)),
    NewState = State#state{entries=NewEntries},
    {reply, ok, NewState}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% Internal Functions 
%%====================================================================

init_file(File, Size) ->
    case repair_file(File, Size) of
        {ok, MetaLocation, Term} ->
            {ok, Meta} = read_metadata(File, MetaLocation),
            {ok, Config} = read_config(File, ConfigLoc),
            {MetaLocation, Meta, Config, Term};
        empty_file ->
            {0, undefined, undefined, 0}
    end.

make_trailer(EntryStart, MetadataStart) ->
    T0 = <<MetadataStart:64, EntryStart:64, ?MAGIC>>,
    Crc = erlang:crc32(T0),
    <<Crc:32, T0>>.

write_entries(File, Entries, Location, MetaLocation) ->
    lists:foldl(fun(#rafter_entry{term=Term, type=Type}=E, {Loc, Count) ->
        Entry = entry_to_binary(E),
        Trailer = make_trailer(Loc, MetaLocation),
        ok = file:write(File, <<Entry/binary, Trailer/binary>>),
        NewLoc = Loc + byte_size(Entry) + ?TRAILER_SIZE,
        update_metadata_config(File, Term, MetaLocation, Loc, NewLoc)
        case counts_toward_index(E) of
            true ->
                {NewLoc, Count + 1};
            false ->
                {NewLoc, Count}
        end
    end, {Location, 0}, Entries).

update_metadata_config(File, Term, MetaStart, ConfigStart, End) ->
    {ok, Meta} = read_metadata(File, MetaStart),
    NewMeta = Meta#meta{config_location=ConfigStart},
    write_metadata(File, Term, End, NewMeta).

counts_toward_index(#rafter_entry{type=config}) ->
    true;
counts_toward_index(#rafter_entry{type=op}) ->
    true;
counts_toward_index(_) ->
    false.

read_config(File, Loc) ->
    {entry, Data, _} = read_entry(File, Location, [?CONFIG]),
    #rafter_entry{type=config, cmd=Config} = binary_to_entry(Data),
    {ok, Config}.

write_metadata(File, Term, Loc, Meta) ->
    E = #rafter_entry{type=meta, term=Term, cmd=Meta},
    Entry = entry_to_binary(E),
    %% Loc is both the start of the entry and the start of the metadata
    Trailer = make_trailer(Loc, Loc),
    ok = file:write(File, <<Entry/binary, Trailer/binary>>).

read_metadata(File, Loc) ->
    {entry, Data, _} = read_entry(File, Location, [?META]),
    #rafter_entry{type=meta, cmd=Meta} = binary_to_entry(Data),
    {ok, Meta}.

truncate(File, Pos) ->
    file:position(File, Pos),
    file:truncate(File).

maybe_truncate(File, TruncateAt, FileSize) ->
    case TruncateAt < FileSize of
        true ->
            ok = truncate(File, TruncateAt);
        false ->
            ok
    end.

repair_file(File, Size) ->
    case scan_for_trailer(File, Size) of
        {ok, MetaStart, EntryStart, TruncateAt} ->
            maybe_truncate(File, TruncateAt, Size),
            {entry, Data, _} = read_entry(File, EntryStart, ?ALL),
            #rafter_entry{type=Type, term=Term} = binary_to_entry(Data), 
            case Type of
                config ->
                    %% At this point we must have written config and crashed before updating
                    %% the metadata. A config entry must always be followed immediately by a
                    %% metadata entry since metadata points to the latest config.
                    update_metadata_config(File, Term, MetaStart, 
                                           EntryStart, TruncateAt),
                    {ok, TruncateAt, Term};
                _ ->
                    {ok, MetaStart, Term}
            end;
        not_found ->
            truncate(File, 0),
            empty_file
    end.

scan_for_trailer(File, Loc) ->
    case find_magic_number(File, Loc) of
        {ok, MagicLoc} ->
            case file:pread(File, MagicLoc - (?TRAILER_SIZE-8), ?TRAILER_SIZE) of
                {ok, <<Crc:32, MetaStart:64, EntryStart:64, ?MAGIC>>} ->
                    case erlang:crc32(<<MetaStart:64, EntryStart:64, ?MAGIC>>) of
                        Crc ->
                            {ok, MetaStart, EntryStart, MagicLoc + 8};
                        _ ->
                            scan_for_trailer(File, MagicLoc)
                    end;
                eof ->
                    not_found
            end;
        not_found ->
            not_found
    end.

read_block(File, Loc) ->
    case Loc < ?READ_BLOCK_SIZE of
        true ->
            {ok, Buffer} = file:pread(File, 0, Loc),
            {Buffer, 0};
        false ->
            Start = Loc - ?READ_BLOCK_SIZE,
            {ok, Buffer} = file:pread(File, Start, ?READ_BLOCK_SIZE),
            {Buffer, Start}
    end.

%% @doc Continuously read blocks from the file and search backwards until the
%% magic number is found or we reach the beginning of the file.
find_magic_number(File, Loc) ->
    {Block, Start} = read_block(File, Loc),
    case find_last_magic_number_in_block(Block) of
        {ok, Offset} ->
            {ok, Loc+Offset};
        not_found ->
            case Start of
                0 ->
                    not_found;
                _ ->
                    %% Ensure we search the overlapping 8 bytes between blocks
                    find_magic_number(File, Start+8)
            end
    end.

-spec find_last_magic_number_in_block(binary()) ->
    {ok, non_neg_integer()} | not_found.
find_last_magic_number_in_block(Block) ->
    case string:rstr(bin_to_list(Block), bin_to_list(?MAGIC)) of
        0 ->
            not_found;
        Index ->
            %% We want the 0 based binary offset, not the 1 based list offset.
            {ok, Index - 1}
    end.

find_latest_config(File) ->
    {ok, scan_latest(File, 0, ?CONFIG)}.

scan_latest(File, Location, Type) ->
    scan_latest(File, Location, Type, not_found).

scan_latest(File, Location, Type, Acc) ->
    case read_entry(File, Location, [Type]) of
            eof ->
                binary_to_entry(Acc);
            {entry, Entry, NewLoc} ->
                scan_latest(File, NewLoc, Type, Entry);
            {skip, NewLoc} ->
                scan_latest(File, NewLoc, Type, Acc)
    end.

%% @doc This function reads the next entry from the log at the given location.
%% It returns the {entry, Entry, NewLocation} if it's one of the requested 
%% types. Otherwise, it returns {skip, NewLocation} where newLocation is the 
%% location of the next entry in the log. If the end of file has been reached 
%% return eof to the client. Errors are fail-fast.
-spec read_entry(file:io_device(), non_neg_integer(), [non_neg_integer()]) ->
    {entry, binary(), non_neg_integer()} | {skip, non_neg_integer()} | eof.
read_entry(File, Location, Types) ->
    case file:pread(File, Location, ?HEADER_SIZE) of
        {ok, <<_Sha1:20/binary, Type:8, _Term:64, DataSize:32>>}=Header ->
            case lists:member(Type, Types) of
                true ->
                    read_data(File, Location + ?HEADER_SIZE, Header);
                false ->
                    NewLocation = Location + ?HEADER_SIZE + DataSize,
                    {skip, NewLocation}
            end;
        eof ->
            eof
    end.

-spec read_data(file:io_device(), non_neg_integer(), binary()) ->
    {entry, binary(), non_neg_integer()} | eof.
read_data(File, Location, <<Sha1:20/binary, Type:8, Term:64, Size:32>>=H) ->
    case file:pread(File, Location + ?HEADER_SIZE, Size) of 
        {ok, Data} ->
            %% Fail-fast Integrity check. TODO: Offer user repair options?
            Sha1 = crypto:hash(sha, <<Type:8, Term:64, Size:32, Data/binary>>),
            NewLocation = Location + ?HEADER_SIZE + Size,
            {entry, <<H/binary, Data/binary>>, NewLocation};
        eof ->
            eof
    end.

find_config(Entries, CurrentConfig) ->
    lists:foldl(
        fun(#rafter_entry{type=config, cmd=Config}, _) ->
                Config;
            (_, Acc) ->
                Acc
        end, CurrentConfig, Entries).

