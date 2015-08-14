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
	gen_server:call(?MODULE, {get, Key}).

get(Key, ReplyTo) ->
	gen_server:call(?MODULE, {get, Key, ReplyTo}).

post(Key, Msg) ->
	gen_server:call(?MODULE, {post, Key, Msg}).

delete(Key) ->
	gen_server:call(?MODULE, {delete, Key}).

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

handle_call({get, Key}, Client, #{list := List} = State) ->
	case lists:keyfind(Key, 1, List) of
		false ->
			Timestamp = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
			{noreply, State#{list => [{Key, [Client], undefined, Timestamp} | List]}};
		{Key, Waiting, undefined, Timestamp} ->
			{noreply, State#{list => lists:keyreplace(Key, 1, {Key, [Client | Waiting], undefined, Timestamp})}};
		{Key, _, Msg, _} ->
			{reply, {ok, Msg}, State}
	end;

handle_call({get, Key, ReplyTo}, _, #{list := List} = State) ->
	case lists:keyfind(Key, 1, List) of
		false ->
			Timestamp = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
			{reply, ok, State#{list => [{Key, ReplyTo, undefined, Timestamp} | List]}};
		{Key, Waiting, undefined, Timestamp} ->
			{reply, ok, State#{list => lists:keyreplace(Key, 1, {Key, [ReplyTo | Waiting], undefined, Timestamp})}};
		{Key, _, Msg, _} ->
			ReplyTo ! {sdm, Key, Msg},
			{reply, ok, State}
	end;

handle_call({post, Key, Msg}, _, #{list := List} = State) ->
	case lists:keyfind(Key, 1, List) of
		false ->
			Timestamp = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
			{reply, ok, State#{list => [{Key, [], Msg, Timestamp} | List]}};
		{Key, [], _, _} ->
			Timestamp = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
			{reply, ok, State#{lists => lists:keyreplace(Key, 1, {Key, [], Msg, Timestamp})}};
		{Key, Waiting, undefined, Timestamp} ->
			ok = mailing(Waiting, Key, Msg),
			{reply, ok, State#{list => lists:keyreplace(Key, 1, {Key, [], undefined, Timestamp})}};
	end;

handle_call({delete, Key}, _, #{list := List} = State) ->
	{reply, ok, State#{list => list:keydelete(Key, 1, List)}};

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

mailing([], _, _) ->
	ok;
mailing([ReplyTo | Waiting], Key, Msg) when is_pid(ReplyTo) ->
	ReplyTo ! {sdm, Key, Msg},
	mailing(Waiting, Key, Msg);
mailing([Client | Waiting], Key, Msg) when is_tuple(Client) ->
	gen_server:reply(Client, {ok, Msg}),
	mailing(Waiting, Key, Msg).