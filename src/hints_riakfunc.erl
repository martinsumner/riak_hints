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
%%% Pre-commit hooks and map functions required to use the hints solution
%%%
%%% The pre-commit hook is the function precommit_eventblock/1
%%%
%%% The pre-commit hook will depend on a project-specific function which can
%%% take the decoded Object value and the Object Key and return -
%%% {ok, Facts, NewIndexes} or {error, Reason}
%%% Facts should be a list of facts to be queryable via the hints file;
%%% NewIndexes should be a list of {index_field, index_term} tuples to be added
%%% to both the original object and the hints object
%%%
%%% The map function is map_checkhints/3
%%%
%%% The map will return a list of {Fact, Key} tuples indicating in which keys
%%% each fact can be found
%%%
%%%
%%% @end
%%% Created : 23. Sep 2015 15:05
%%%-------------------------------------------------------------------
-module(hints_riakfunc).
-author("martin").

-define(HINTS_BUCKET, "hints").
-define(INFO, "INFO").
-define(WARN, "WARN").
-define(ERROR, "ERROR").
-define(MD_DELETE, <<"X-Riak-Deleted">>).
-define(MD_INDEX, <<"index">>).
-define(PROJECT_EDFP(DecodedObj, ObjectKey),
  objections_audit:extract_data_forprocess(DecodedObj, ObjectKey)).
%% API
-export([precommit_eventblock/1, map_checkhints/3]).

-import(mochijson2, [decode/1]).

-include_lib("eunit/include/eunit.hrl").

%% TODO: Consider splitting the hints files into multiple objects
%% If the hints file is split (perhaps into eight separate objects), when
%% querying for a single fact there will only be an eighth of the data required
%% to be lift from disk.  Obviously conjunction queries would become harder.
%%
%% Ultimtely with a good file-system cache the advantage of a split hints
%% file may be negated.

%% TODO: Need to experiment with alternative JSON decode
%% i.e. https://kivikakk.ee/2013/05/20/erlang_is_slow.html

%% PRE_COMMIT_HOOK

%% The precommithook for the event block should
%% 1. Extract some facts to use as the basis of hints
%% 2. Extract some new metadata to set any indexes
%% 3. Create the new HintsObject, and modify the metadata of this and the
%% original object
%% 4. Load a hints object into Riak
%% 5. Return the original Object (now updated)
%%
%% Pre-commit hooks should not be applied to tombstones (so deleted objects
%% are filtered at the top)

precommit_eventblock(Object) ->
  JsonFile = riak_object:get_value(Object),
  ObjectKey = riak_object:key(Object),
  Metadata = riak_object:get_metadata(Object),
  hints_utility:writelog("Applying pre-commit hook to object with Key=~s~n",
    [binary_to_list(ObjectKey)], ?INFO),
  case dict:find(?MD_DELETE, Metadata) of
    {ok, "true"} ->
      hints_utility:writelog("Deletion detected by hook for key=~w~n",
        [ObjectKey], ?INFO),
      Object;
    _ ->
      Result = decode_object(JsonFile),
      case Result of
        {ok, {struct, DecodedObj}} ->
          Data = ?PROJECT_EDFP(DecodedObj, ObjectKey),
          case Data of
            {ok, Facts, NewIndexes} ->
              HintsObject = generate_hintsobject(Facts, ObjectKey, NewIndexes),
              RplObject = set_indexes(Object, NewIndexes),
              load_hints(HintsObject),
              hints_utility:writelog("Pre-commit hook complete for object"
              ++ " with Key=~s~n",
                [binary_to_list(ObjectKey)], ?INFO),
              RplObject;
            {error, Reason} ->
              {fail, "Invalid File: Unable to extract data to process"
                ++ Reason}
          end;
        {error, Reason} ->
          {fail, Reason}
      end
  end.


decode_object(JsonFile) ->
  try
    {ok, mochijson2:decode(JsonFile)}
  catch
    throw:invalid_utf8 ->
      {error, "Invalid JSON: Illegal UTF-8 character"};
    error:_ ->
      {error, "Invalid JSON"}
  end.

generate_hintsobject(Facts, ObjectKey, NewIndexes) ->
  HintsObj = hints_bloom:create_bloom(Facts),
  ObjectBucket = list_to_binary(?HINTS_BUCKET),
  RiakHintsObj = riak_object:new(ObjectBucket, ObjectKey, HintsObj),
  set_indexes(RiakHintsObj, NewIndexes).

%% Update the metadata with the new indexes (which should be a list
%% of {field, value} tuples

set_indexes(Object, NewIndexes) ->
  Metadata = riak_object:get_metadata(Object),
  UpdatedMD = add_indexes(Metadata, NewIndexes),
  riak_object:update_metadata(Object, UpdatedMD).

add_indexes(Metadata, NewIndexes) ->
  case dict:find(?MD_INDEX, Metadata) of
    {ok, OldIndexes} ->
      CombinedIndexes = NewIndexes ++ OldIndexes;
    error ->
      CombinedIndexes = NewIndexes
  end,
  dict:store(?MD_INDEX, CombinedIndexes, Metadata).

load_hints(RplHintsObject) ->
  {ok, C} = riak:local_client(),
  %% Store the object
  C:put(RplHintsObject).


%% MAP FUNCTION
%%
%% Map module to check a hints file
%% Should take a fact (or multiple facts) and output a list of
%% {Fact, key} tuples
%%

%% TODO: Support more complicated queries (i.e. conjunction of Facts)

map_checkhints({error, notfound}, KeyData, Facts) ->
  %% If not found assume the fact is present, send a potentially false positive
  %% To achieve this the Key needs to be passed from the query as KeyData as
  %% well as Key.  Otherwise a false positive is not returned
  hints_utility:writelog("Object not found and KeyData=~w~n",
    [KeyData], ?WARN),
  case KeyData of
    undefined ->
      [];
    _ ->
      lists:foldl(fun(Fact, Output) -> [{Fact, KeyData}|Output] end, [], Facts)
  end;
map_checkhints(Hints, _KeyData, Facts) ->
  ObjectKey = riak_object:key(Hints),
  hints_utility:maybelog("Check hints map job is processing Key=~s~n",
    [binary_to_list(ObjectKey)], ?INFO),
  HintsBins = riak_object:get_values(Hints),
  checkallhints(HintsBins, Facts, [], ObjectKey).

checkallhints([], _Facts, Results, _ObjectKey) ->
  lists:usort(Results);
checkallhints([HintsBin|Tail], Facts, Results, ObjectKey) ->
  checkallhints(Tail, Facts,
    lists:append(Results, checkhints(HintsBin, Facts, Results, ObjectKey)),
  ObjectKey).

checkhints(_HintsBin, [], Results, _ObjectKey) ->
  Results;
checkhints(HintsBin, [Fact|Tail], Results, ObjectKey) ->
  case hints_bloom:check_key(Fact, HintsBin) of
    true ->
      checkhints(HintsBin, Tail,[{Fact, ObjectKey}|Results], ObjectKey);
    _ ->
      checkhints(HintsBin, Tail, Results, ObjectKey)
  end.

