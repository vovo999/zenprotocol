module Tally.Tally

module Result = Infrastructure.Result
module Option = Infrastructure.Option

open Consensus
open Types
open Checked
open Consensus.Crypto
open Consensus.Serialization.Serialization
open UtxoSet

type allocation = byte

type payout = Recipient * Spend list

type PK = Crypto.PublicKey

type PKHash = Hash.Hash

type CoinbaseRatio =
    | CoinbaseRatio of byte

type T =
    {
        coinbaseRatio : Map<CoinbaseRatio, uint64>
        payout        : Map<payout, uint64>
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

let private (>>=) = FSharpx.Option.(>>=)

let private (|@>) x f = Option.map f x

let check (b : bool) : Option<unit> =
    option { if b then return () }

let (|<-) (x : Option<'a>) (y : 'b) : Option<'b> =
    x |@> fun _ -> y

let (|<--) (x : Option<'a>) (y : Lazy<'b>) : Option<'b> =
    x |> Option.map (fun _ -> y.Force())

let ( *>) : Option<'a> -> Option<'b> -> Option<'b> =
    FSharpx.Option.( *>)

let ignoreResult (x : Option<'a>) : Option<unit> =
    x |<- ()

let allocationToCoinbaseRatio (allocation : allocation) : CoinbaseRatio =
    CoinbaseRatio (100uy - allocation)

let coinbaseRatioToAllocation (CoinbaseRatio coinbaseRatio : CoinbaseRatio) : allocation =
    (100uy - coinbaseRatio)

let getRatio (CoinbaseRatio x) = x

let mapMapKeys f m = m |> Map.toSeq |> Seq.map (fun (k,x) -> (f k, x)) |> Map.ofSeq

let accumulate (key : 'a) (value : uint64) (map : Map<'a,uint64>) : Map<'a,uint64> =
    let oldValue = Map.tryFind key map |> Option.defaultValue 0UL
    map |> if value > 0UL then Map.add key (oldValue + value) else id

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

let validatePercentage (allocation : Allocation) : Option<Allocation> =
    check (allocation <= 100uy)
    |<- allocation

let validateAllocation env allocation =
    Some allocation
    >>= validatePercentage
    |@> allocationToCoinbaseRatio
    >>= validateCoibaseRatio env
    |@> coinbaseRatioToAllocation

// Iteratively remove the payout spends from the fund and check that it wasn't depleted
let validatePayout (env : Env) ((_, spends) as vote : payout) : Option<payout> =
    
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
    match vote with
    | Allocation allocation ->
        allocation
        |> validateAllocation env
        |@> Allocation
    | Payout (pay,out) ->
        (pay,out)
        |> validatePayout env
        |@> Payout

let addVote (env : Env) (tally:T) (vote:Ballot) amount : T =
    validateVote env vote >>= function
    | Allocation allocation ->
        let voteCoinbaseRatio = allocationToCoinbaseRatio allocation
        Some { tally with coinbaseRatio = accumulate voteCoinbaseRatio amount tally.coinbaseRatio }
    | Payout (pay,out) ->
        let votePayout = (pay,out)
        Some { tally with payout = accumulate votePayout amount tally.payout}
    |> Option.defaultValue tally

let integrateMaps (amounts : Map<'key1,uint64>) (keys : Map<'key1,'key2>) : Map<'key2,uint64> =
    
    let addToMap (optKey : Option<'a>) (amount : uint64) (m : Map<'a, uint64>) : Map<'a, uint64> =
        optKey
        |@> fun key -> accumulate key amount m
        |> Option.defaultValue m
    
    let addAmount (acc : Map<'key2,uint64>) (oldKey : 'key1) (amount : uint64) : Map<'key2,uint64> =
        keys
        |> Map.tryFind oldKey
        |> fun key -> addToMap key amount acc
    
    Map.fold addAmount Map.empty amounts

let integrateBallots (balances : Map<PKHash, uint64>) (votes : Map<PK, 'a>) : Map<'a, uint64> =
    votes
    |> mapMapKeys PublicKey.hash
    |> integrateMaps balances

let mergeBallots (env : Env) (allocationVotes : Map<allocation, uint64>) (payoutVotes : Map<payout, uint64>) : T =
    let allocationBallots     = mapMapKeys Allocation allocationVotes 
    let payoutBallots         = mapMapKeys Payout     payoutVotes
    let collect ballots tally = Map.fold (addVote env) tally ballots
    
    empty
    |> collect allocationBallots
    |> collect payoutBallots

let createTally (env : Env) (balances : Map<PKHash, uint64>) (allocationBallots : Map<PK, allocation>) (payoutBallots : Map<PK, payout>) : T =
    let allocationVotes = integrateBallots balances allocationBallots
    let payoutVotes     = integrateBallots balances payoutBallots
    mergeBallots env allocationVotes payoutVotes

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
            |> mapMapKeys coinbaseRatioToAllocation
            |> getAllocationResult
    
        let payout =
            tally.payout
            |> getPayoutResult
    
        Some {allocation=allocation; payout=payout}
