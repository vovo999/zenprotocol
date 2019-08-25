module Tally.Tally

module Result = Infrastructure.Result
module Option = Infrastructure.Option

open Consensus
open Types
open Checked
open UtxoSet

type Allocation = byte

type CoinbaseRatio =
    | CoinbaseRatio of byte

type T =
    {
        coinbaseRatio : Map<byte, uint64>
        payout        : Map<Recipient * Spend list, uint64>
    }

type Env =
    {
        coinbaseCorrectionCap : CoinbaseRatio
        lowerCoinbaseBound    : CoinbaseRatio
        lastCoinbaseRatio     : CoinbaseRatio
        lastFund              : Fund
    }

let empty : T =
    {
        coinbaseRatio = Map.empty
        payout        = Map.empty
    }

let isEmpty = (=) empty

let option = FSharpx.Option.maybe

let liftNone (optRes : Option<Option<'a>>) : Option<Option<'a>> =
    match optRes with
    | None          -> Some None
    | Some (Some x) -> Some (Some x)
    | Some None     -> None

let ignoreResult (x : Option<'a>) : Option<unit> =
    Option.map (fun _ -> ()) x

let check (b : bool) : Option<unit> =
    option { if b then return () }

let (|<-) (x : Option<'a>) (y : 'b) : Option<'b> =
    x |> Option.map (fun _ -> y)

let (|<--) (x : Option<'a>) (y : Lazy<'b>) : Option<'b> =
    x |> Option.map (fun _ -> y.Force())

let ( *>) : Option<'a> -> Option<'b> -> Option<'b> =
    FSharpx.Option.( *>)

let allocationToCoinbaseRatio (allocation : Allocation) : CoinbaseRatio =
    CoinbaseRatio (100uy - allocation)

let coinbaseRatioToAllocation (CoinbaseRatio coinbaseRatio : CoinbaseRatio) : Allocation =
    (100uy - coinbaseRatio)

let getRatio (CoinbaseRatio x) = x

let accumulate (key : 'a) (value : uint64) (map : Map<'a,uint64>) : Map<'a,uint64> =
    let defaultValue =
        Map.tryFind key map
        |> Option.defaultValue 0UL
    let newValue = defaultValue + value
    
    if newValue = 0UL then
        Map.remove key map
    else
        Map.add key newValue map

let validateCoibaseRatio (env : Env) (CoinbaseRatio coinbaseRatio : CoinbaseRatio) : Option<CoinbaseRatio> =
    
    let lastCoinbaseRatio = env.lastCoinbaseRatio     |> getRatio |> uint16
    let correctionCap     = env.coinbaseCorrectionCap |> getRatio |> uint16
    let globalRatioMin    = env.lowerCoinbaseBound    |> getRatio
    
    let localRatioMin =
        byte <| (lastCoinbaseRatio * correctionCap) / 100us
    
    let localRatioMax =
        byte <| (lastCoinbaseRatio * 100us) / correctionCap
    
    let ratioMin =
        max globalRatioMin localRatioMin
    
    let ratioMax =
        min 100uy localRatioMax
    
    check (ratioMin <= coinbaseRatio && coinbaseRatio <= ratioMax)
    |<- CoinbaseRatio coinbaseRatio

let validateAllocation env allocation =
    Some allocation
    |> Option.map allocationToCoinbaseRatio
    |> Option.bind (validateCoibaseRatio env)
    |> Option.map coinbaseRatioToAllocation

// Iteratively remove the payout spends from the fund and check that it wasn't depleted
let validatePayout (env : Env) ((_, spends) as vote : Recipient * List<Spend>) : Option<Recipient * List<Spend>> =
    
    let subtractSpend (fund : Fund) (spend : Spend) : Option<Fund> =
        option {
            let! fundAmount =
                Map.tryFind spend.asset fund
            
            let! updatedAmount =
                check (fundAmount >= spend.amount)
                |<-- lazy (fundAmount - spend.amount)
            
            return Map.add spend.asset updatedAmount fund
        }
    
    let checkNonZero (spend : Spend) : Option<unit> = 
        check (spend.amount > 0UL)
    
    let checkSpendableFunds : Option<unit> =
        spends
        |> FSharpx.Option.foldM subtractSpend env.lastFund 
        |> ignoreResult
    
    let checkNonZeros : Option<unit> =
        spends
        |> FSharpx.Option.mapM checkNonZero
        |> ignoreResult
    
    let checkSize : Option<unit> =
        check (List.length spends <= 100)
    
    Some ()
    *> checkNonZeros
    *> checkSpendableFunds
    *> checkSize
    |<- vote

let validateVote (env : Env) (vote : Ballot) : Option<Ballot> =
    option {
        let! allocation =
            vote.allocation
            |> Option.map (validateAllocation env)
            |> liftNone    // *
        
        let! payout =
            vote.payout
            |> Option.map (validatePayout env)
            |> liftNone    // *
        
        return {allocation=allocation; payout=payout}
    }
    // * Seperate failure from indifference:
    //     If a field is None then it won't fail the vote validation,
    //     but if it is (Some x) and x is invalid then the whole vote will be invalid.
    //     The validation of each field will create a value of the Option<Option<t>> type
    //     where the internal Option is for indifference and the external Option is for validation failure.
    //     Binding both fields together will only affect the external Option, which is for validation -
    //     so if one field fails both of them will fail but otherwise you'll just get back the original
    //     values of the fields.

let addVote (env : Env) (vote:Ballot) amount (tally:T) : T =
    option {
        let! vote = validateVote env vote
        
        let addToMap x m =
            x
            |> Option.map (fun key -> accumulate key amount m)
            |> Option.defaultValue m
        
        let voteCoinbaseRatio =
            vote.allocation
        
        return {
            coinbaseRatio = addToMap voteCoinbaseRatio tally.coinbaseRatio
            payout        = addToMap vote.payout       tally.payout
        }
    }
    |> Option.defaultValue tally

let addOutputStatus (env : Env) (tally : T) (_ : Outpoint) (outputStatus : OutputStatus) : T =
    match outputStatus with
    | Unspent {lock = Vote (vote, _); spend = spend} when spend.asset = Asset.Zen ->
        addVote env vote spend.amount tally
    | _ ->
        tally

let addUnspentVotes (env : Env) (utxos : Map<Outpoint, OutputStatus>) : T =
    utxos
    |> Map.fold (addOutputStatus env) empty

// Naive weighted median by sorting and searching (assumes the list of votes is nonempty) 
let private weightedMedian (votes : seq<byte * uint64>) : byte =
    let sortedVotes   = Seq.sortBy fst votes
    let weights       = Seq.map snd sortedVotes
    let totalWeight   = weights |> Seq.sum
    let weightsBefore = Seq.scan (+) 0UL         weights
    let weightsAfter  = Seq.scan (-) totalWeight weights
    let coupledVotes  = Seq.zip3 weightsBefore sortedVotes (Seq.tail weightsAfter)
    let cond (b,_,a)  = 2UL * b <= totalWeight  &&  2UL * a <= totalWeight
    let l, wl         = Seq.find     cond coupledVotes |> fun (_,x,_) -> x
    let h, wh         = Seq.findBack cond coupledVotes |> fun (_,x,_) -> x
    byte ((uint64 l * wl + uint64 h * wh) / (wl + wh))

#if DEBUG
let weightedMedianTest = weightedMedian
#endif

let private getResultWith aggregator map =
    if Map.isEmpty map then
        None
    else
        map
        |> Map.toSeq
        |> aggregator
        |> Some

let getAllocationResult =
    getResultWith weightedMedian

let getPayoutResult =
    getResultWith (Seq.maxBy snd >> fst)

let getWinner tally =
    if isEmpty tally then
        None
    else 
        let allocation =
            tally.coinbaseRatio
            |> getAllocationResult
    
        let payout =
            tally.payout
            |> getPayoutResult
    
        Some {allocation=allocation; payout=payout}:Ballot option
