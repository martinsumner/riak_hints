# riak_hints

This is not generic, reusable, code at present.  This is written for one specific project and palced here for sharing purposes.

The idea is to support a situation when large objects are loaded into Riak containing lots (>1000, but <100000) of individual facts.  The facts are sparsely populated across the objects (most objects don't have any give fact) - and there is a need to find quickly in which objects the fact can be found.

To speed this up there is a pre-commit hook which will create and load a hints file on receipt of the object.  The hook pulls the facts out of the object, and then creates a bloom filter representing the facts.  The bloom filter is compressed using rice-encoding with a single hash to minimise the footprint, and partitioned to minimise the query time.

A map function can then be used where a list of facts can be added as "arg", and then the hints files can be queried outputting [{Fact, ObjectKey}] when one may have been found.  
