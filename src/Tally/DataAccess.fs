module Tally.DataAccess

open Infrastructure
open DataAccess
open Consensus
open UtxoSet
open Types
open Tally.Serialization
open Blockchain.Serialization
open Consensus.Serialization.Serialization



[<Literal>]
let DbVersion = 1


type T = {
    pkbalance: Collection<Interval, PKBalance>
    allocationVoters: Collection<Interval, PKAllocation>
    payoutVoters: Collection<Interval, PKPayout>
    winner: Collection<Interval, Winner>
    funds: Collection<Interval, Fund>
    tip: SingleValue<Tip>
    voteUtxo: Collection<Interval, VoteUtxo>
    dbVersion: SingleValue<int>
    allocations: Collection<Interval, byte>
}

let createContext dataPath =
    Platform.combine dataPath "tally"
    |> DatabaseContext.create DatabaseContext.Medium


let init databaseContext =

    use session = DatabaseContext.createSession databaseContext
    let pkbalance = Collection.create session "pkbalance" VarInt.serialize PKBalance.serialize PKBalance.deserialize
    let allocationVoters = Collection.create session "allocationVoters" VarInt.serialize PKAllocation.serialize PKAllocation.deserialize
    let payoutVoters = Collection.create session "payoutVoters" VarInt.serialize PKPayout.serialize PKPayout.deserialize
    let winner = Collection.create session "winners" VarInt.serialize Winner.serialize Winner.deserialize
    let funds = Collection.create session "funds" VarInt.serialize Fund.serialize Fund.deserialize
    let tip = SingleValue.create databaseContext "blockchain" Tip.serialize Tip.deserialize
    let voteUtxo = Collection.create session "voteUtxo" VarInt.serialize VoteUtxo.serialize VoteUtxo.deserialize
    let dbVersion = SingleValue.create databaseContext "dbVersion" Version.serialize Version.deserialize
    let allocations = Collection.create session "allocations" VarInt.serialize Allocation.serialize Allocation.deserialize

    match SingleValue.tryGet dbVersion session with
    | None ->
            SingleValue.put dbVersion session DbVersion
    | Some DbVersion -> 
        ()
    | Some version ->  
        failwithf "Tally: wrong db version, expected %d but got %d" DbVersion version
        
    let t = {
        pkbalance = pkbalance
        allocationVoters = allocationVoters
        payoutVoters = payoutVoters
        winner = winner
        funds = funds
        tip = tip
        voteUtxo = voteUtxo
        dbVersion = dbVersion
        allocations = allocations
    }

    Session.commit session
    t

let dispose t =
    Disposables.dispose t.pkbalance

module Tip =
    let put t = SingleValue.put t.tip
    let tryGet t = SingleValue.tryGet t.tip
    let get t session = tryGet t session |> Option.get

module PKBalance =
    let get t = Collection.get t.pkbalance

    let tryGet t = Collection.tryGet t.pkbalance
    let put t = Collection.put t.pkbalance

    let delete t = Collection.delete t.pkbalance
    let truncate t = Collection.truncate t.pkbalance

    let contains t = Collection.containsKey t.pkbalance
    
module PKAllocation =
    let get t = Collection.get t.allocationVoters

    let tryGet t = Collection.tryGet t.allocationVoters
    let put t = Collection.put t.allocationVoters

    let delete t = Collection.delete t.allocationVoters
    let truncate t = Collection.truncate t.allocationVoters

    let contains t = Collection.containsKey t.allocationVoters
 
module VoteUtxoSet =
    let get t = Collection.get t.voteUtxo

    let tryGet t = Collection.tryGet t.voteUtxo
    let put t = Collection.put t.voteUtxo

    let delete t = Collection.delete t.voteUtxo
    let truncate t = Collection.truncate t.voteUtxo

    let contains t = Collection.containsKey t.voteUtxo
    
module PKPayout =
    let get t = Collection.get t.payoutVoters

    let tryGet t = Collection.tryGet t.payoutVoters
    let put t = Collection.put t.payoutVoters

    let delete t = Collection.delete t.payoutVoters
    let truncate t = Collection.truncate t.payoutVoters

    let contains t = Collection.containsKey t.payoutVoters

module Fund =
    let get t = Collection.get t.funds

    let tryGet t = Collection.tryGet t.funds
    let put t = Collection.put t.funds

    let delete t = Collection.delete t.funds
    let truncate t = Collection.truncate t.funds

    let contains t = Collection.containsKey t.funds


module Winner =
    let get t = Collection.get t.winner
    
    let tryGet t = Collection.tryGet t.winner
    let put t = Collection.put t.winner
    
    let delete t = Collection.delete t.winner
    let truncate t = Collection.truncate t.winner
    
    let contains t = Collection.containsKey t.winner
    
module Allocation =
    let get t = Collection.get t.allocations
    
    let tryGet t = Collection.tryGet t.allocations
    let put t = Collection.put t.allocations
    
    let delete t = Collection.delete t.allocations
    let truncate t = Collection.truncate t.allocations
    
    let contains t = Collection.containsKey t.allocations