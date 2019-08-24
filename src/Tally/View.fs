module Tally.View


open Consensus
open Chain
open DataAccess
open Infrastructure
open Tally.Serialization
open Types
open UtxoSet


type T =
    {
        voteUtxoSet: Map<Outpoint, OutputStatus>
        tally: Tally.T
        winner: Option<Ballot>
    }
    

let empty = {
    voteUtxoSet = Map.empty
    tally = Tally.empty
    winner = None
}
      
module Fund =
    let get dataAccess session interval =
        Fund.tryGet dataAccess session interval
        |> Option.defaultValue Map.empty
        

module Winner =
    let get dataAccess session view interval tally =
         match view.winner with
         | Some winner ->
             Some winner
         | None -> 
             match Winner.tryGet dataAccess session interval with 
             | Some winner ->
                 Some winner
             | None ->
                 Tally.getWinner tally

module Tally =
    open Tally

    let get dataAccess session view (chainParams:ChainParameters) voteUtxo interval =
        
        let lastFund =
            if interval <> 0ul then
                Fund.get dataAccess session (interval - 1ul)
            else
                Map.empty

        
        let lastAllocation =
            Winner.tryGet dataAccess session interval
            |> Option.map (fun allocation -> allocation.allocation )
            |> Option.defaultValue (Some 0uy)
            |> Option.defaultValue 0uy
        
        let env =
            {
                allocationCorrectionCap = chainParams.allocationCorrectionCap
                lowerAllocationBound    = chainParams.lowerAllocationBound
                lastFund                = lastFund
                lastAllocation          = lastAllocation
            }
        
        if Tally.isEmpty view.tally then
            Tally.addUnspentVotes env voteUtxo
        else view.tally
        
module VoteUtxoSet =
    let get dataAccess session view interval =
        if Map.isEmpty view.voteUtxoSet then
            VoteUtxoSet.tryGet dataAccess session interval
            |> Option.defaultValue view.voteUtxoSet
        else view.voteUtxoSet

let getWinner dataAccess session interval view =
    match Winner.tryGet dataAccess session interval with
    | Some winner ->
         Some winner
    | None ->
        Tally.getWinner view.tally

let show dataAccess session chainParams interval view =
    
    let voteUtxo = 
        VoteUtxoSet.get dataAccess session view interval
    
    let tally =
        Tally.get dataAccess session view chainParams voteUtxo interval
        
    let winner = getWinner dataAccess session interval view
    match winner with
    | Some winner ->
         Winner.put dataAccess session interval winner
    | None -> ()

    {voteUtxoSet=voteUtxo; tally=tally; winner= winner}:T
