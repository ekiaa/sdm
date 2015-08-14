-module(sdm).

-bihaviour(gen_server).

-export([start/0, start_link/0, get/1, get/2, post/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%====================================================================
%% API functions
%%====================================================================

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) ->
	get(Key, self()).

get(Key, Pid) ->
	gen_server:call(?MODULE, {get, Key, Pid}).

post(Key, Msg) ->
	gen_server:call(?MODULE, {post, Key, Msg}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Description: Initiates the server
%%--------------------------------------------------------------------

init([]) ->
	ClearingInterval = application:get_env(sdm, clearing_interval, 60000),
	MessageLifetime = application:get_env(sdm, message_lifetime, 60),
	timer:send_interval(ClearingInterval, clear),
	{ok, #{list => [], lifetime => MessageLifetime}}.

%%--------------------------------------------------------------------
%% Description: Handling call messages
%%--------------------------------------------------------------------

handle_call({get, Key, WaitingPid}, _, #{list := List} = State) ->
	case lists:keyfind(Key, 1, List) of
		false ->
			Timestamp = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
			{reply, ok, State#{list => [{Key, WaitingPid, undefined, Timestamp} | List]}};
		{Key, undefined, Msg, _} ->
			WaitingPid ! {sdm, Key, Msg},
			{reply, ok, State#{list => lists:keydelete(Key, 1, List)}};
		{Key, _, undefined, _} ->
			{reply, {error, already_waited}, State}
	end;

handle_call({post, Key, Msg}, _, #{list := List} = State) ->
	case lists:keyfind(Key, 1, List) of
		false ->
			Timestamp = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
			{reply, ok, State#{list => [{Key, undefined, Msg, Timestamp} | List]}};
		{Key, undefined, _, _} ->
			{reply, {error, already_sended}, State};
		{Key, WaitingPid, undefined, _} ->
			WaitingPid ! {sdm, Key, Msg},
			{reply, ok, State#{list => lists:keydelete(Key, 1, List)}}
	end;

handle_call(Request, From, State) ->
	Error = {error, {?MODULE, ?LINE, {From, Request}}},
	{stop, Error, Error, State}.

%%--------------------------------------------------------------------
%% Description: Handling cast messages
%%--------------------------------------------------------------------

handle_cast(Message, State) ->
	{stop, {error, {?MODULE, ?LINE, Message}}, State}.

%%--------------------------------------------------------------------
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------

handle_info(clear, #{list := List, lifetime := MessageLifetime} = State) ->
	TimestampNow = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
	Fun = fun
		({_, _, _, Timestamp}, Acc) when TimestampNow - Timestamp > MessageLifetime -> Acc;
		(Rec, Acc) -> [Rec | Acc]
	end,
	{noreply, State#{list => lists:foldl(Fun, [], List)}};

handle_info(Info, State) ->
	{stop, {error, {?MODULE, ?LINE, Info}}, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%%--------------------------------------------------------------------

terminate(normal, _) ->	ok;
terminate(Reason, State) -> lager:error("terminate:~nReason: ~p~nState: ~p", [Reason, State]), ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
