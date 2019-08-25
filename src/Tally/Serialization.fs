module Tally.Serialization

open Consensus
open Serialization
open Serialization

open Tally.Types
open Ballot.Payout
open Blockchain
open Messaging.Services

module Tally =
    let size = fun (tally:Tally.T) ->
        Map.size (fun (_, votes) -> 
            Byte.size + 
            Amount.size votes) tally.coinbaseRatio +
        Map.size (fun ((recipient, spend), votes) -> 
            Recipient.size recipient + 
            List.size Spend.size spend + 
            Amount.size votes) tally.payout

    let write (stream:Stream) = fun (tally:Tally.T) ->
        Map.write (fun stream (allocation, votes)->
            Byte.write stream allocation
            Amount.write stream votes) stream (tally.coinbaseRatio)
        Map.write (fun stream ((recipient, spend), votes)->
            Recipient.write stream recipient
            List.write Spend.write stream spend
            Amount.write stream votes) stream tally.payout

    let read (stream:Stream) =
        {
            coinbaseRatio = Map.read (fun stream -> Byte.read stream, Amount.read stream) stream
            payout = Map.read (fun stream -> Ballot.Payout.read stream, Amount.read stream) stream
        } : Tally.T
        
    let serialize = serialize size write
    let deserialize = deserialize read

module Tip =
    let size _ =
        Hash.size + 4

    let write stream tip =
        Hash.write stream tip.blockHash
        stream.writeNumber4 tip.blockNumber

    let read stream =
        let blockHash = Hash.read stream
        let blockNumber = stream.readNumber4 ()

        {
            blockHash = blockHash
            blockNumber = blockNumber
        }

    let serialize = serialize size write
    let deserialize = deserialize read

module Fund =
    let size fund =
        Map.size (fun (asset, amount) -> 
            Asset.size asset + 
            Amount.size amount) fund
        
    let write (stream:Stream) = fun (fund:Fund) ->
        Map.write (fun stream (asset, amount)->
                Asset.write stream asset
                Amount.write stream amount) stream fund
    let read (stream:Stream) =
        Map.read (fun stream -> Asset.read stream, Amount.read stream) stream
        
    let serialize = serialize size write
    let deserialize = deserialize read
    
module VoteUtxo =
    open Blockchain.Serialization
    
    let size voteUtxo =
        Map.size (fun (outpoint, outputStatus) -> 
            Outpoint.size outpoint + 
            OutputStatus.size outputStatus) voteUtxo
    let write (stream:Stream) = fun (voteUtxo:VoteUtxo) ->
        Map.write (fun stream (outpoint, outputStatus)->
                Outpoint.write stream outpoint
                OutputStatus.write stream outputStatus) stream voteUtxo
    let read (stream:Stream) =
        Map.read (fun stream -> Outpoint.read stream, OutputStatus.read stream) stream
        
    let serialize = serialize size write
    let deserialize = deserialize read