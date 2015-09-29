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

%% @doc
%% Used for creating fixed-size self-regulating encoded bloom filters
%%
%% Normally a bloom filter in order to achieve optimium size increases the
%% number of hashes as the desired false positive rate increases.  There is
%% a processing overhead for checking this bloom, both because of the number
%% of hash calculations required, and also because of the need to CRC check
%% the bloom to ensure a false negative result is not returned due to
%% corruption.
%%
%% A more space efficient bloom can be achieved through the compression of
%% bloom filters with less hashes (and in an optimal case a single hash).
%% This can be achieved using rice encoding.
%%
%% Rice-encoding and single hash blooms are used here in order to provide an
%% optimally space efficient solution, but also as the processing required to
%% support uncompression can be concurrently performing a checksum role.
%%
%% For this to work, the bloom is divided into n partitions, with the number
%% of partitions calculated based on the length of the key list to be held.
%% A hash is taken which is in a range from 1 to 256 * DIVISOR * n; where 256
%% is in this case the approximate maximum number of keys to be mapped to a
%% slot, and the DIVISOR represents the approximate reciprical of the false
%% positive rate.
%%
%% The bloom is then created by calculating the differences between the ordered
%% elements of the hash list and representing the difference using an exponent
%% and a 13-bit remainder (where the number of bits is based on the power of 2
%% whihc matches the DIVISOR - 2 ^ 13 = 8192) i .e.
%% 8000  ->   0  11111 01000000
%% 10000 ->  10  00000 00010000
%% 20000 -> 110  01110 00100000
%%
%% Each bloom should have approximately a maximum of 256 differences (based on
%% the size of the hash range.
%%
%% Fronting the bloom is a bloom index, formed first by 16 pairs of 3-byte
%% max hash, 2-byte length (bits) - with then each of the encoded bitstrings
%% appended.  The max hash is the  total of all the differences (which should
%% be the highest hash in the bloom).  Then the whole bloom is prepended with
%% the slot count (actually the slot count - 1) so the number of slots can be
%% determined at query time.
%%
%% To check a key against the bloom, hash it, take the four least signifcant
%% bits and read the start pointer, max hash end pointer from the expected
%% positions in the bloom index.  Then roll through from the start pointer to
%% the end pointer, accumulating each difference. There is a possible match if
%% either the accumulator hits the expected hash or the max hash doesn't match
%% the final accumulator (to cover if the bloom has been corrupted by a bit
%% flip somwhere). A miss is more than twice as expensive (on average) than a
%% potential match - but still only requires asmall number of  integer
%% additions.
%%
%% For 2048 keys, this takes up <4KB.  The false positive rate is 0.000122
%% This compares favourably for the equivalent size optimal bloom which
%% would require 11 hashes and have a false positive rate of 0.000459.
%% Checking with a positive match should take on average about 6 microseconds,
%% and a negative match should take around 11 microseconds.
%%
%% See ../test/rice_test.erl for proving timings and fpr.
%%
%% If there are more than 2 ^ 16 keys in the list, then access times will
%% increase, and false positive rates increase - as the number of keys in the
%% list increases
%%
%% The serialised form starts with a single byte giving the slot count - 1
%% There
%%
%% @end

%% TODO: Write spec entries

-module(hints_bloom).

-export([create_bloom/1,
  check_key/2,
  check_allkeys/2,
  confirm_fpr/2,
  key_generator/1]).

-import(mochijson2, [encode/1]).

-include_lib("eunit/include/eunit.hrl").

-define(DIVISOR_BITS, 13). % 2 ^ DIVISOR_BITS should be the Divisor
-define(DIVISOR, 8192).
% False positive rate is always better than the inverse of the Divisor
% The DIVISOR cannot exceed 2 ^ 16 (due to format of binary)
-define(DEFAULT_FORMAT, binary_format).

%% Create a bitstring representing the bloom filter from a key list

create_bloom(KeyList) ->
  create_bloom(KeyList, ?DEFAULT_FORMAT).

create_bloom(KeyList, Format) ->
  % Slot count set to try and keep the counts to less than 256 keys per slot
  % without increasing the number of slots (and hence associated storage
  % inefficiency) needlessly
  case length(KeyList) of
    Length when Length < 2048 ->
      Slots = 8;
    Length when Length < 4096 ->
      Slots = 16;
    Length when Length < 8192 ->
      Slots = 32;
    Length when Length < 16384 ->
      Slots = 64;
    Length when Length < 32768 ->
      Slots = 128;
    _ ->
      Slots = 256
  end,
  MaxHash = 256 * ?DIVISOR * Slots,
  create_bloom(KeyList, Slots, MaxHash, Format).

create_bloom(KeyList, SlotCount, MaxHash, Format) ->
  HashLists = array:new(SlotCount, [{default, []}]),
  OrdHashLists = create_hashlist(KeyList, HashLists, SlotCount, MaxHash),
  % io:format("Ready to serialise bloom ~n"),
  serialise_bloom(OrdHashLists, Format).


%% Checking for a key

check_allkeys([], _) ->
  true;
check_allkeys([Key|Rest], BitStr) ->
  case check_key(Key, BitStr) of
    false ->
      false;
    true ->
      check_allkeys(Rest, BitStr)
  end.

check_whichkeys(KeyList, Bloom) ->
  check_whichkeys(KeyList, Bloom, []).

check_whichkeys([], _, Acc) ->
  Acc;
check_whichkeys([Key|Tail], Bloom, Acc) ->
  case check_key(Key, Bloom) of
    true ->
      check_whichkeys(Tail, Bloom, [Key|Acc]);
    false ->
      check_whichkeys(Tail, Bloom, Acc)
  end.


check_key(Key, BitStr) ->
  <<SlotInt:8/integer, RemBitStr/bitstring>> = BitStr,
  check_key(Key, RemBitStr, SlotInt + 1, ?DIVISOR_BITS, ?DIVISOR).

check_key(Key, BitStr, SlotCount, Factor, Divisor) ->
  MaxHash = 256 * ?DIVISOR * SlotCount,
  {Slot, Hash} = get_slothash(Key, MaxHash, SlotCount),
  {StartPos, Length, TopHash} = find_position(Slot, BitStr, 0, 40 * SlotCount),
  case BitStr of
    <<_:StartPos/bitstring, Bloom:Length/bitstring, _/bitstring>> ->
      check_hash(Hash, Bloom, Factor, Divisor, 0, TopHash);
    _ ->
      io:format("Possible corruption of bloom index ~n"),
      true
  end.

find_position(Slot, BloomIndex, Counter, StartPosition) ->
  <<TopHash:24/integer, Length:16/integer, Rest/bitstring>> = BloomIndex,
  case Slot of
    Counter ->
      {StartPosition, Length, TopHash};
    _ ->
      find_position(Slot, Rest, Counter + 1, StartPosition + Length)
  end.


% Checking for a hash within a bloom

check_hash(_, <<>>, _, _, Acc, MaxHash) ->
  case Acc of
    MaxHash ->
      false;
    _ ->
      io:format("Failure of CRC check on bloom filter~n"),
      true
  end;
check_hash(HashToCheck, BitStr, Factor, Divisor, Acc, TopHash) ->
  case findexponent(BitStr) of
    {ok, Exponent, BitStrTail} ->
      case findremainder(BitStrTail, Factor) of
        {ok, Remainder, BitStrTail2} ->
          NextHash = Acc + Divisor * Exponent + Remainder,
          case NextHash of
            HashToCheck ->
              true;
            _ ->
              check_hash(HashToCheck, BitStrTail2, Factor,
                Divisor, NextHash, TopHash)
          end;
        error ->
          io:format("Failure of CRC check on bloom filter~n"),
          true
      end;
    error ->
      io:format("Failure of CRC check on bloom filter~n"),
      true
  end.

%% Convert the key list into an array of sorted hash lists

create_hashlist([], HashLists, _, _) ->
  HashLists;
create_hashlist([HeadKey|Rest], HashLists, SlotCount, MaxHash) ->
  {Slot, Hash} = get_slothash(HeadKey, MaxHash, SlotCount),
  HashList = array:get(Slot, HashLists),
  create_hashlist(Rest,
    array:set(Slot, [Hash|HashList], HashLists), SlotCount, MaxHash).

%% Convert an array of hash lists into an serialsed bloom

serialise_bloom(HashLists, Format) ->
  SlotCount = array:size(HashLists),
  serialise_bloom(HashLists, SlotCount, 0,  [], Format).

serialise_bloom(HashLists, SlotCount, Counter, Blooms, Format) ->
  case Counter of
    SlotCount ->
      finalise_bloom(Blooms, SlotCount, Format);
    _ ->
      Bloom = serialise_singlebloom(lists:usort(array:get(Counter,
        HashLists))),
      serialise_bloom(HashLists, SlotCount, Counter + 1, [Bloom|Blooms],
        Format)
  end.

serialise_singlebloom(HashList) ->
  serialise_singlebloom(HashList, <<>>, 0, ?DIVISOR, ?DIVISOR_BITS).

serialise_singlebloom([], BloomStr, TopHash, _, _) ->
  % io:format("Single bloom created with bloom of ~w and top hash of ~w~n", [BloomStr, TopHash]),
  {BloomStr, TopHash};
serialise_singlebloom([Hash|Rest], BloomStr, TopHash, Divisor, Factor) ->
  HashGap = Hash - TopHash,
  Exp = buildexponent(HashGap div Divisor),
  Rem = HashGap rem Divisor,
  NewBloomStr = <<BloomStr/bitstring, Exp/bitstring, Rem:Factor/integer>>,
  serialise_singlebloom(Rest, NewBloomStr, Hash, Divisor, Factor).


finalise_bloom(Blooms, SlotCount, binary_format) ->
  finalise_bloom_binary(Blooms, SlotCount);
finalise_bloom(_, _, json_format) ->
  {error, not_implemented}.


finalise_bloom_binary(Blooms, SlotCount) ->
  finalise_bloom_binary(Blooms, SlotCount, {<<>>, <<>>}).

finalise_bloom_binary([], SlotCount, BloomAcc) ->
  {BloomIndex, BloomStr} = BloomAcc,
  Length = bit_size(BloomIndex) + bit_size(BloomStr),
  TailToAdd = (8 - Length rem 8) rem 8,
  SlotInt = SlotCount - 1,
  <<SlotInt:8/integer, BloomIndex/bitstring,
  BloomStr/bitstring, <<0>>:TailToAdd/bitstring>>;
finalise_bloom_binary([Bloom|Rest], SlotCount, BloomAcc) ->
  {BloomStr, TopHash} = Bloom,
  {BloomIndexAcc, BloomStrAcc} = BloomAcc,
  Length = bit_size(BloomStr),
  UpdIdx = <<TopHash:24/integer, Length:16/integer, BloomIndexAcc/bitstring>>,
  % io:format("Adding bloom string of ~w to bloom~n", [BloomStr]),
  UpdBloomStr = <<BloomStr/bitstring, BloomStrAcc/bitstring>>,
  finalise_bloom_binary(Rest, SlotCount, {UpdIdx, UpdBloomStr}).


buildexponent(Exponent) ->
  buildexponent(Exponent, <<0:1>>).

buildexponent(0, OutputBits) ->
  OutputBits;
buildexponent(Exponent, OutputBits) ->
  buildexponent(Exponent - 1, <<1:1, OutputBits/bitstring>>).

findexponent(BitStr) ->
  findexponent(BitStr, 0).

findexponent(<<>>, _) ->
  error;
findexponent(<<H:1/integer, T/bitstring>>, Acc) ->
  case H of
    1 -> findexponent(T, Acc + 1);
    0 -> {ok, Acc, T}
  end.

findremainder(BitStr, Factor) ->
  case BitStr of
    <<Remainder:Factor/integer, BitStrTail/bitstring>> ->
      {ok, Remainder, BitStrTail};
    _ ->
      error
  end.


get_slothash(Key, MaxHash, SlotCount) ->
  Hash = erlang:phash2(Key, MaxHash),
  {Hash rem SlotCount, Hash div SlotCount}.


%%%%%%%%%%%%%%%%
% T E S T
%%%%%%%%%%%%%%%

corrupt_bloom(Bloom) ->
  Length = bit_size(Bloom),
  Random = random:uniform(Length),
  <<Part1:Random/bitstring, Bit:1/integer, Rest1/bitstring>> = Bloom,
  case Bit of
    1 ->
      <<Part1/bitstring, 0:1/integer, Rest1/bitstring>>;
    0 ->
      <<Part1/bitstring, 1:1/integer, Rest1/bitstring>>
  end.

key_generator(Counter) ->
  key_generator(Counter, false).

key_generator(Counter, UseBigKey) ->
  Characters = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"],
  key_generator(Counter, [], Characters, UseBigKey).

key_generator(0, KeyList, _, _) ->
  KeyList;
key_generator(Counter, KeyList, Characters, UseBigKey) ->
  case UseBigKey of
    true ->
      NewKey = {list_to_binary(build_randomstring(8, Characters, "")),
        list_to_binary(build_randomstring(64, Characters, ""))};
    _ ->
      NewKey = list_to_binary(build_randomstring(8, Characters, ""))
  end,
  key_generator(Counter - 1, [NewKey|KeyList], Characters, UseBigKey).

build_randomstring(0, _, Acc) ->
  Acc;
build_randomstring(RequiredLength, Characters, Acc) ->
  NewChr = lists:nth(random:uniform(length(Characters)), Characters),
  build_randomstring(RequiredLength - 1, Characters, Acc ++ NewChr).


confirm_fpr(KeysToTestCount, BloomSize) ->
  KeyList = lists:usort(key_generator(BloomSize + KeysToTestCount)),
  {KeysToTest, KeysForBloom} = lists:split(KeysToTestCount, KeyList),
  Bloom = create_bloom(KeysForBloom),
  MatchedKeys = length(check_whichkeys(KeysToTest, Bloom)),
  io:format("Bloom tested and roughly ~w out of ~w false matches.~n",
    [MatchedKeys, KeysToTestCount]),
  io:format("Bloom contained ~w keys and was ~w bytes.~n",
    [length(KeysForBloom), size(Bloom)]).


-ifdef(TEST).

tinybloom_test() ->
  KeyList = ["key1", "key2", "key3", "key4"],
  Bloom = create_bloom(KeyList),
  io:format("Bloom of ~w of length ~w ~n", [Bloom, bit_size(Bloom)]),
  ?assertMatch(true, check_key("key1", Bloom)),
  ?assertMatch(true, check_key("key2", Bloom)),
  ?assertMatch(true, check_key("key3", Bloom)),
  ?assertMatch(true, check_key("key4", Bloom)),
  ?assertMatch(false, check_key("key5", Bloom)).

bigbloom_test() ->
  KeyList = key_generator(20000),
  Bloom = create_bloom(KeyList),
  lists:foreach(fun(Key) -> ?assertMatch(true, check_key(Key, Bloom)) end,
    KeyList).

notsobigbloom1_test() ->
  KeyList = key_generator(1000),
  Bloom = create_bloom(KeyList),
  lists:foreach(fun(Key) -> ?assertMatch(true, check_key(Key, Bloom)) end,
    KeyList).

notsobigbloom2_test() ->
  KeyList = key_generator(1000, true),
  Bloom = create_bloom(KeyList),
  lists:foreach(fun(Key) -> ?assertMatch(true, check_key(Key, Bloom)) end,
    KeyList).

superbigbloom_test() ->
  KeyList = key_generator(80000),
  Bloom = create_bloom(KeyList),
  {KeyListToTest, _Tail} = lists:split(1000, KeyList),
  lists:foreach(fun(Key) -> ?assertMatch(true, check_key(Key, Bloom)) end,
    KeyListToTest).

bloom_corruption_test() ->
  KeyList = ["key1", "key2", "key3", "key4"],
  Bloom = create_bloom(KeyList),
  Bloom1 = corrupt_bloom(Bloom),
  ?assertMatch(true, check_allkeys(KeyList, Bloom1)),
  Bloom2 = corrupt_bloom(Bloom),
  ?assertMatch(true, check_allkeys(KeyList, Bloom2)),
  Bloom3 = corrupt_bloom(Bloom),
  ?assertMatch(true, check_allkeys(KeyList, Bloom3)),
  Bloom4 = corrupt_bloom(Bloom),
  ?assertMatch(true, check_allkeys(KeyList, Bloom4)),
  Bloom5 = corrupt_bloom(Bloom),
  ?assertMatch(true, check_allkeys(KeyList, Bloom5)),
  Bloom6 = corrupt_bloom(Bloom),
  ?assertMatch(true, check_allkeys(KeyList, Bloom6)).

-endif.