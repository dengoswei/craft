1. leader collected the commited state of replicate
   => determin if needed to send empty AppendEntries msg to replicate to 
      update the commited_index

2. class Raft

3. store_seq;

4. updateReplicateState => refresh hb timeout ?


## Rocksdb + raft

## Membership change

conf change:
1. apply conf change in checkAndAppend... 
   => setting pending conf
   => ignore conf chang req if any pending conf;

2. clear pending state once conf entry has been commited !

## test with google benchmark

## add random drop message in SendHelper (testing)

## read persistant state to re-build raft

### TODAY
## decouple replicate_tracker & raft_impl
