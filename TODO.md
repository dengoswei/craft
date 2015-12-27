1. leader collected the commited state of replicate
   => determin if needed to send empty AppendEntries msg to replicate to 
      update the commited_index

2. class Raft

3. store_seq;

4. updateReplicateState => refresh hb timeout ?


## Rocksdb + raft

## Membership change

## test with google benchmark

## add random drop message in SendHelper (testing)

## read persistant state to re-build raft

