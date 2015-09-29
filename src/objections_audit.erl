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
%%% Specific functions required for processing in the Patient Objections
%%% Audit service
%%%
%%% @end
%%% Created : 23. Sep 2015 15:05
%%%-------------------------------------------------------------------


-module(objections_audit).
-author("martin").

-define(HINTS_BUCKET, "hints").
-define(JOB_INDEX, "jobid_bin").
-define(FILE_VERSION, 1).
-define(INFO, "INFO").
-define(WARN, "WARN").
-define(ERROR, "ERROR").
-define(VALID_OBJECTION_EVENTS, [<<"ObjectionNotFoundEvent">>,
  <<"ObjectionFoundEvent">>, <<"ObjectionExemptEvent">>]).
-define(EVENT_ERROR_THRESHOLD, 10).
%% API
-export([extract_data_forprocess/2]).

-import(mochijson2, [decode/1]).

-include_lib("eunit/include/eunit.hrl").


%% extract_data_forprocess should return either {ok, Facts, NewMetadata} or
%% {error, Reason}

extract_data_forprocess(DecodedObj, ObjectKey) ->
  Ver = ebfextract_version(DecodedObj, ObjectKey),
  case Ver of
    {ok, Version} ->
      JID = ebfextract_jobid(DecodedObj, ObjectKey, Version),
      case JID of
        {ok, JobID} ->
          EBs = ebfextract_eventblocks(DecodedObj, ObjectKey, Version),
          case EBs of
            {ok, EventBlocks} ->
              IDs = extract_identities(EventBlocks, ObjectKey, Version),
              case IDs of
                {ok, Facts} ->
                  {ok, Facts, [{?JOB_INDEX, JobID}]};
                Error ->
                  Error
              end;
            Error ->
              Error
          end;
        Error ->
          Error
      end;
    Error ->
      Error
  end.


%% Functions to extract details from the events block file
%% Each function should be passsed the object and the file version (except
%% for the extract_version function)
%% If the extract changes for a given version then override the default
%% clause for that version.  Also, the Key should be passed - to allow
%% traceability for errors
%%
%% Extract functions should return {ok, Result} if and only if the Result has
%% been verified to be of the right type (plus potentially other business
%% rules}.  Otherwise error should be returned to prompt a badmatch upstream

ebfextract_version(EventBlockFile, ObjectKey) ->
  Version = proplists:get_value(<<"Version">>, EventBlockFile),
  case Version of
    1 ->
      {ok, Version};
    _ ->
      writelog("Unexpected version ~w for Event Block File with Key ~w~n",
        [Version, ObjectKey], ?WARN),
      {error, "Bad version"}
  end.

ebfextract_jobid(EventBlockFile, _ObjectKey, _Version) ->
  JobProcessingElement = proplists:get_value(<<"JobProcessingMD">>,
    EventBlockFile),
  case JobProcessingElement of
    undefined ->
      {error, "Missing Job Processing Element"};
    {struct, JobProcessingMD}->
      JobID = proplists:get_value(<<"GlobalJobId">>, JobProcessingMD),
      case JobID of
        undefined ->
          {error, "Missing JobID"};
        _ ->
          {ok, JobID}
      end
  end.

ebfextract_eventblocks(EventBlockFile, ObjectKey, _Version) ->
  EventBlocks = proplists:get_value(<<"EventBlocks">>, EventBlockFile),
  case EventBlocks of
    [{struct, HeadBlock}|_] when is_list(HeadBlock) ->
      {ok, EventBlocks};
    _ ->
      writelog("Unexpected format of Event Block File with Key ~w~n ",
        [ObjectKey], ?WARN),
      {error, "Bad Event Block"}
  end.


extract_identities(EventBlocks, ObjectKey, _Version) ->
  extract_identities(EventBlocks, ObjectKey, _Version, []).

extract_identities([], _ObjectKey, _Version, IDList) ->
  {ok, IDList};
extract_identities([{struct, HeadBlock}|Tail], ObjectKey, _Version, IDList) ->
  EventArray = proplists:get_value(<<"EventArray">>, HeadBlock),
  UserType = proplists:get_value(<<"IdentifierUsedType">>, HeadBlock),
  EventType  = proplists:get_value(<<"EventType">>, HeadBlock),
  case {UserType, EventType} of
    {undefined, _} ->
      writelog("Missing UserType in Event Block Metadata for File with Key ~w~n",
        [ObjectKey], ?WARN),
      {error, "Missing IdentifiedUsedType"};
    {_, undefined} ->
      writelog("Missing EventType in Event Block Metadata for File with Key ~w~n",
        [ObjectKey], ?WARN),
      {error, "Missing EventType"};
    {<<"NHSNumber">>, <<"ObjectionNotFoundEvent">>} ->
      extract_identities(Tail, ObjectKey, _Version,
        lists:append(IDList, EventArray));
    {_, _} ->
      case lists:member(EventType, ?VALID_OBJECTION_EVENTS) of
        true ->
          {ok, StrippedIDList} = strip_identities(EventArray, ObjectKey),
          extract_identities(Tail, ObjectKey, _Version,
            lists:append(IDList, StrippedIDList));
        _ ->
          writelog("Invalid Usertype=~w or EventType=~w~n",
            [UserType, EventType], ?WARN),
          {error, "Invalid IdentifiedUsedType or EventType"}
      end
  end.


%% Stripping the identities will assume the NHS Number is the first 10
%% characters in the string.
%% Validate NHS Numbers stripped?

strip_identities(EventArray, ObjectKey) ->
  strip_identities(EventArray, ObjectKey, [], 0).

%% TODO: Validate the concept of thresholds for accepting errors on strip

strip_identities([], ObjectKey, StrippedIDList, ErrorCount) ->
  case ErrorCount of
    0 ->
      {ok, StrippedIDList};
    _ when ErrorCount div length(StrippedIDList) > ?EVENT_ERROR_THRESHOLD ->
      writelog("Error count on strippping exceeds threshold Key=~w~n",
        [ObjectKey], ?ERROR),
      writelog("ErrorCount=~w and SuccessCount=~w~n",
        [ErrorCount, length(StrippedIDList)], ?INFO),
      {error, "Too many invalid NHSNumbers"};
    _ ->
      writelog("Error count nonzero on stripping but below threshold Key=~w~n",
        [ObjectKey], ?WARN),
      writelog("ErrorCount=~w and SuccessCount=~w~n",
        [ErrorCount, length(StrippedIDList)], ?INFO),
      {ok, StrippedIDList}
  end;
strip_identities([IDString|Tail], ObjectKey, StrippedIDList, ErrorCount) ->
  case extract_nhsnumber(IDString) of
    {ok, NHSNumber} ->
      strip_identities(Tail, ObjectKey, [NHSNumber|StrippedIDList], ErrorCount);
    {error, Reason} ->
      writelog("Identity rejected due to reason ~w~n", [Reason], ?INFO),
      strip_identities(Tail, ObjectKey, StrippedIDList, ErrorCount + 1)
  end.

%% The NHSNumber may be in a string with other appended details
%% e.g. <<"9876543210|AltID">> or <<"9876543210|ObjID">>
%% These details need to be stripped to leave just the NHS Number

extract_nhsnumber(IDString) when is_binary(IDString) ->
  Lngth = length(binary_to_list(IDString)),
  case Lngth of
    _ when Lngth < 10 ->
      {error, bad_length};
    _ ->
      {NHSNumber, _} = lists:split(10, binary_to_list(IDString)),
      case validate_nhsnumber(NHSNumber) of
        true ->
          {ok, list_to_binary(NHSNumber)};
        false ->
          {error, bad_checksum}
      end
  end;
extract_nhsnumber(_IDString) ->
  {error, bad_format}.


validate_nhsnumber(Digits) ->
  validate_nhsnumber(Digits, 0, 10).

validate_nhsnumber(Digits, Acc, _Mult) when length(Digits) == 1 ->
  CheckDigit = 11 - Acc rem 11,
  case string:to_integer(string:substr(Digits, 1, 1)) of
    {CheckDigit, []} ->
      true;
    {0, []} when CheckDigit == 11 ->
      true;
    _ ->
      false
  end;
validate_nhsnumber(Digits, Acc, Mult) ->
  {Int, []} = string:to_integer(string:substr(Digits, 1, 1)),
  validate_nhsnumber(string:sub_string(Digits, 2), Acc + Int * Mult, Mult - 1).


%% Helper function to make it easier to switch to lager as context changes

writelog(Text, Inputs, ErrorLevel) ->
  io:format(ErrorLevel ++ ": " ++ Text, Inputs).

%%%%%%%%%%%%%%%%
% T E S T
%%%%%%%%%%%%%%%

-ifdef(TEST).

valid_extract_test() ->
  {ok, EBFFile} = file:read_file("../test/validEventBlockFile.json"),
  {struct, EventBlockFile} = mochijson2:decode(EBFFile),
  ?assertMatch({ok, 1},
    ebfextract_version(EventBlockFile, "TestKey")),
  ?assertMatch({ok, <<"e7415570-2b6f-4bca-a46b-12f293e69040">>},
    ebfextract_jobid(EventBlockFile, "TestKey", 1)),
  ?assertMatch({ok, _},
    ebfextract_eventblocks(EventBlockFile, "TestKey", 1)).

completely_invalid_json_test() ->
  EBFFile = "{\"WrongStuff\": 1}",
  {struct, EventBlockFile} = mochijson2:decode(EBFFile),
  ?assertMatch({error, _},
    ebfextract_version(EventBlockFile, "TestKey")),
  ?assertMatch({error, _},
    ebfextract_jobid(EventBlockFile, "TestKey", 1)),
  ?assertMatch({error, _},
    ebfextract_eventblocks(EventBlockFile, "TestKey", 1)).

nhsNumber_validity_test() ->
  ?assertMatch(true, validate_nhsnumber("9999999999")),
  ?assertMatch(true, validate_nhsnumber("9999999980")),
  ?assertMatch(false, validate_nhsnumber("9999999998")).

nhsNumber_extract_test() ->
  ?assertMatch({ok, <<"9999999999">>},
    extract_nhsnumber(<<"9999999999|ABC">>)),
  ?assertMatch({ok, <<"9999999980">>},
    extract_nhsnumber(<<"9999999980|ABC">>)),
  ?assertMatch({error, bad_checksum},
    extract_nhsnumber(<<"9999999998|ABC">>)),
  ?assertMatch({error, bad_length},
    extract_nhsnumber(<<"999999999">>)),
  ?assertMatch({error, bad_format},
    extract_nhsnumber("9999999999|ABC")).

block_extract_test() ->
  {ok, EBFFile} = file:read_file("../test/eventBlockFileTypeTest.json"),
  {struct, EventBlockFile} = mochijson2:decode(EBFFile),
  {ok, EventBlocks} = ebfextract_eventblocks(EventBlockFile, 'TestKey', 1),
  {ok, IDList} = extract_identities(EventBlocks, 'TestKey', 1),
  ?assertMatch(true, lists:member(<<"5016275971">>, IDList)), % In pseduo id list
  ?assertMatch(true, lists:member(<<"0574095985">>, IDList)), % In straight number list
  ?assertMatch(true, lists:member(<<"6382307564">>, IDList)), % In objections list
  ?assertMatch(false, lists:member(<<"9999999999">>, IDList)).

invalidblock_test() ->
  {ok, EBFFile} = file:read_file("../test/invalidEBF1.json"),
  {struct, EventBlockFile} = mochijson2:decode(EBFFile),
  {ok, EventBlocks} = ebfextract_eventblocks(EventBlockFile, 'TestKey', 1),
  ?assertMatch({error, _}, extract_identities(EventBlocks, 'TestKey', 1)).

full_extract_data_test() ->
  {ok, EBFFile} = file:read_file("../test/eventBlockFileTypeTest.json"),
  {struct, EventBlockFile} = mochijson2:decode(EBFFile),
  {ok, Facts, Index} = extract_data_forprocess(EventBlockFile, 'TestKey'),
  ?assertMatch(true, lists:member(<<"5016275971">>, Facts)),
  ?assertMatch([{?JOB_INDEX, <<"e7415570-2b6f-4bca-a46b-12f293e69040">>}], Index).

full_extract_invalidblock_test() ->
  {ok, EBFFile} = file:read_file("../test/invalidEBF1.json"),
  {struct, EventBlockFile} = mochijson2:decode(EBFFile),
  Result = extract_data_forprocess(EventBlockFile, 'TestKey'),
  ?assertMatch({error, "Invalid IdentifiedUsedType or EventType"}, Result).

-endif.