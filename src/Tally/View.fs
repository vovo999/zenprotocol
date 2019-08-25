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
        tally: Tally.T
        winner: Option<Winner>
    }
    

let empty = {
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
    open Messaging.Services
    open Tally

    let get dataAccess session view (chainParams:ChainParameters) interval =
        
        let lastFund =
            if interval <> 0ul then
                Fund.get dataAccess session (interval - 1ul)
            else
                Map.empty

        
        let lastAllocation =
            Winner.tryGet dataAccess session interval
            |> Option.bind (fun allocation -> allocation.allocation )
            |> Option.defaultValue 0uy
        
        let env =
            {
                coinbaseCorrectionCap   = Tally.allocationToCoinbaseRatio chainParams.allocationCorrectionCap
                lowerCoinbaseBound      = Tally.allocationToCoinbaseRatio chainParams.upperAllocationBound
                lastCoinbaseRatio       = Tally.allocationToCoinbaseRatio lastAllocation
                lastFund                = lastFund
            }
        let balance =
            DataAccess.PKBalance.tryGet dataAccess session interval
            |> Option.defaultValue Map.empty           
        let allocation =
            PKAllocation.tryGet dataAccess session interval
            |> Option.defaultValue Map.empty
        let payout =
            PKPayout.tryGet dataAccess session interval
            |> Option.defaultValue Map.empty
        
        
        if Tally.isEmpty view.tally then
            Tally.createTally env balance allocation payout
        else view.tally
        

let getWinner dataAccess session interval view =
    match Winner.tryGet dataAccess session interval with
    | Some winner ->
         Some winner
    | None ->
        Tally.getWinner view.tally

let show dataAccess session chainParams interval view =

    let tally =
        Tally.get dataAccess session view chainParams interval
        
    let winner = getWinner dataAccess session interval view
    match winner with
    | Some winner ->
         Winner.put dataAccess session interval winner
    | None -> ()
    
    {tally=tally; winner= winner}:T
