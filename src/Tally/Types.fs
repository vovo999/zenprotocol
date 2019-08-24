module Tally.Types

open Consensus
open UtxoSet
open Crypto
open Types
open Hash

type Tip =
    {
        blockHash: Hash
        blockNumber: uint32
    }
    

type Interval = uint32


type Fund = Map<Asset,uint64>

type PKHash = Hash.Hash

type PKBalance = Map<PKHash,uint64>

type Allocation = byte
type Payout = Recipient * Spend list

type PKAllocation= Map<PublicKey,Allocation>
type PKPayout = Map<PublicKey,Payout>


type VoteUtxo = Map<Outpoint,OutputStatus>
