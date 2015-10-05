%%%-------------------------------------------------------------------
%%% @author martin
%%% @copyright (C) 2015, Martin Sumner
%%% Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
%%% documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
%%% rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
%%% permit persons to whom the Software is furnished to do so, subject to the following conditions:
%%% The above copyright notice and this permission notice shall be included in all copies or substantial portions of
%%% the Software.
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
%%% THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
%%% TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%
%%% Created : 26. May 2015 13:57
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% Helper functions for logging to make life easier when running locally with
%%% no lager available to help
%%%
%%% @end
%%% Created : 05. Oct 2015 11:36
%%%-------------------------------------------------------------------
-module(hints_utility).
-author("martin").

%% Compile with local if no lager installed for logging
%% i.e. c(hints_riakfunc, {d, local})
-ifdef(local).
-define(LOGGING_FRAMEWORK, io).
-else.
-compile([{parse_transform, lager_transform}]).
-define(LOGGING_FRAMEWORK, lager).
-endif.

%% API
-export([writelog/3, maybelog/3]).

-define(INFO, "INFO").
-define(WARN, "WARN").
-define(ERROR, "ERROR").
-define(LOG_PROBABILITY, 1).


%% Helper function to make it easier to switch to lager as context changes

maybelog(Text, Inputs, ErrorLevel) ->
  case random:uniform(?LOG_PROBABILITY) of
    1 ->
      writelog(Text, Inputs, ErrorLevel);
    _ ->
      ok
  end.

writelog(Text, Inputs, ErrorLevel) ->
  case ?LOGGING_FRAMEWORK of
    io ->
      io:format(ErrorLevel ++ ": " ++ Text, Inputs);
    lager ->
      case ErrorLevel of
        ?INFO ->
          lager:info(Text, Inputs);
        ?WARN ->
          lager:warning(Text, Inputs);
        ?ERROR ->
          lager:error(Text, Inputs)
      end;
    _ ->
      io:format("ERROR: Unexpeted logging framework of ~w~n",
        [?LOGGING_FRAMEWORK])
  end.