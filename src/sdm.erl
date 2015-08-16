-module(sdm).

-bihaviour(gen_server).

-export([start/0, start_link/0, post/2, get_sync/1, get_sync/2, get_async/1, get_async/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEF_MSG_TTL, 30000).

%%====================================================================
%% API functions
%%====================================================================

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

post(Key, Msg) ->
	post(Key, Msg, ?DEF_MSG_TTL).

post(Key, Msg, Timeout) ->
	gen_server:call(?MODULE, {post, Key, Msg, self(), Timeout}, round(1.5 * Timeout)).

get_sync(Key) ->
	get_sync(Key, ?DEF_MSG_TTL).

get_sync(Key, Timeout) ->
	gen_server:call(?MODULE, {get_sync, Key, Timeout}, round(1.5 * Timeout)).

get_async(Key) ->
	get_async(Key, ?DEF_MSG_TTL).

get_async(Key, Timeout) ->
	gen_server:cast(?MODULE, {get_async, Key, self(), Timeout}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Description: Initiates the server
%%--------------------------------------------------------------------

init([]) ->
	{ok, #{}}.

%%--------------------------------------------------------------------
%% Description: Handling call messages
%%--------------------------------------------------------------------

handle_call({get_sync, Key, Timeout}, Client, Messages) ->
	case maps:get(Key, Messages, undefined) of
		undefined ->
			TTL = application:get_env(sdm, msgttl, Timeout),
			timer:send_after(TTL, {timeout, Key}),
			{noreply, maps:put(Key, {client, Client}, Messages)};
		{message, Message, From} ->
			From ! {sdm, Key, sent},
			{reply, {ok, Message}, maps:remove(Key, Messages)};
		_ ->
			{reply, {error, already_waiting}, Messages}
	end;

handle_call({post, Key, Message, From, Timeout}, _, Messages) ->
	case maps:get(Key, Messages, undefined) of
		undefined ->
			timer:send_after(Timeout, {timeout, Key}),
			{reply, ok, maps:put(Key, {message, Message, From}, Messages)};
		{client, Client} ->
			gen_server:reply(Client, {ok, Message}),
			From ! {sdm, Key, sent},
			{reply, ok, maps:remove(Key, Messages)};
		{replyto, ReplyTo} ->
			ReplyTo ! {sdm, Key, {ok, Message}},
			From ! {sdm, Key, sent},
			{reply, ok, maps:remove(Key, Messages)};
		_ ->
			{reply, {error, already_posted}, Messages}
	end;

handle_call(_Request, _From, Messages) ->
	{reply, {error, not_matched}, Messages}.

%%--------------------------------------------------------------------
%% Description: Handling cast messages
%%--------------------------------------------------------------------

handle_cast({get_async, Key, ReplyTo, Timeout}, Messages) ->
	case maps:get(Key, Messages, undefined) of
		undefined ->
			timer:send_after(Timeout, {remove, Key}),
			{noreply, maps:put(Key, {replyto, ReplyTo}, Messages)};
		{message, Message, From} ->
			ReplyTo ! {sdm, Key, {ok, Message}},
			From ! {sdm, Key, sent},
			{noreply, maps:remove(Key, Messages)};
		_ ->
			ReplyTo ! {sdm, Key, {error, already_waiting}},
			{noreply, Messages}
	end;

handle_cast(_Message, Messages) ->
	{noreply, Messages}.

%%--------------------------------------------------------------------
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------

handle_info({timeout, Key}, Messages) ->
	case maps:get(Key, Messages, undefined) of
		undefined ->
			{noreply, Messages};
		{message, _Message, From} ->
			From ! {sdm, Key, timeout},
			{noreply, maps:remove(Key, Messages)};
		{client, Client} ->
			gen_server:reply(Client, {error, timeout}),
			{noreply, maps:remove(Key, Messages)};
		{replyto, ReplyTo} ->
			ReplyTo ! {sdm, Key, {error, timeout}},
			{noreply, maps:remove(Key, Messages)};
		_ ->
			{noreply, Messages}
	end;

handle_info(_Info, Messages) ->
	{noreply, Messages}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%%--------------------------------------------------------------------

terminate(_Reason, _Messages) -> 
	ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
