module Tally.Repository

open Consensus
open Consensus.Chain
open Consensus.Crypto
open Crypto
open UtxoSet
open Types
open Hash
open Logary.Message
open Infrastructure
open Messaging.Services
open Tally
open DataAccess
open Serialization
open View
open Wallet
open Zen.Types.Data
open System.Text
open Tally.DataAccess
open Tally.DataAccess
open Functional

module S = Serialization
module CryptoSig = Crypto.Signature

type Action =
    | Undo
    | Add 

type SyncAction = Action * Block * Hash

let empty =
    {
        blockHash = Hash.zero
        blockNumber = 0ul
    }

module DA =
    
    open FreeDataAccess
    open Free
    open FreeDataAccess.Operators
    
    type DataAccess<'a> = FreeDA<'a>
    
    let dataAccess = freeDA
    
    let private getVoteUtxo interval : DataAccess<VoteUtxo> = dataAccess {
        match! VoteUtxo.tryGet interval with
        | Some x ->
            return x
        | None when interval >= 1u ->
            return!
                VoteUtxo.tryGet (interval - 1u)
                |@> Option.defaultValue Map.empty
        | None ->
            return Map.empty
    }
    
    module Tip =
        
        let update block blockHash =
            Tip.put {blockNumber = block.header.blockNumber; blockHash = blockHash}
    
    module PKBalance =
        
        let spendablePk validMaturity =
            function
            | Unspent {lock=PK pkHash; spend={asset=asset;amount=amount}}
                when asset = Asset.Zen ->
                    Some (pkHash,amount)
            | Unspent {lock=Coinbase (blockNumber,pkHash); spend={asset=asset;amount=amount}}
                when asset = Asset.Zen && validMaturity >= blockNumber ->
                    Some (pkHash,amount)
            | _ ->
                    None
        
        let private getSpendablePks validMaturity (utxos: Map<Outpoint,OutputStatus>) : (Hash * uint64) list =
            utxos
            |> Map.toList
            |> List.map snd
            |> List.choose (spendablePk validMaturity)
        
        let update interval chainParams = dataAccess {
            let getBalance address =
                PKBalance.tryGet interval
                |@> (Option.bind (Map.tryFind address))
                |@> Option.defaultValue 0UL
            
            let validMaturity = CGP.getSnapshotBlock chainParams interval - chainParams.coinbaseMaturity
            
            let updateBalance (pkHash : Hash, amount : uint64) : DataAccess<unit> = dataAccess {
                let! balance = getBalance pkHash
                
                return! PKBalance.tryGet interval
                |@> Option.defaultValue Map.empty
                |@> Map.add pkHash (balance + amount)
                >>= PKBalance.put interval
            }
            
            return! getVoteUtxo interval
            |@> getSpendablePks validMaturity
            >>= iter updateBalance
        }        
    
    module PKBallot =
        
        let private add
            (tryGet : Interval -> DataAccess<Map<Crypto.PublicKey,'a> option>)
            (interval : Interval)
            (pk : Crypto.PublicKey)
            (x : 'a)
            : DataAccess<Map<Crypto.PublicKey,'a>> = dataAccess {
                let! map =
                    tryGet interval
                    |@> Option.defaultValue Map.empty
                
                match Map.tryFind pk map with
                | Some _ -> return map
                | None   -> return Map.add pk x map
            }
        
        let private set
            (tryGet : Interval -> DataAccess<Map<Crypto.PublicKey,'a> option>)
            (put : Interval -> Map<Crypto.PublicKey,'a> -> DataAccess<unit>)
            (interval : Interval)
            (x : 'a)
            (sigs : Crypto.PublicKey list)
            : DataAccess<unit> =
                sigs
                |> iter (fun pk -> add tryGet interval pk x >>= put interval)
        
        let private setAllocation =
            set PKAllocation.tryGet PKAllocation.put
        
        let private setPayout =
            set PKPayout.tryGet PKPayout.put
        
        let private setBallot interval : Ballot -> PublicKey list -> DataAccess<unit> =
            function
            | Ballot.Allocation allocation ->
                setAllocation interval allocation
            | Ballot.Payout (recipient, spends) ->
                setPayout interval (recipient,spends)
        
        let votingWitnesses chainParams =
            function
            | ContractWitness {contractId=cid; command=cmd; messageBody=msgBody} when cid = chainParams.votingContractId ->
                Some (cmd, msgBody)
            | _ ->
                None
        
        let update interval chainParams block =
            let blockNumber = block.header.blockNumber
            block.transactions
            |> List.concatMap (fun ex -> ex.tx.witnesses)
            |> List.choose (votingWitnesses chainParams)
            |> List.choose (uncurry <| VoteParser.parseMessageBody chainParams blockNumber)
            |> iter (uncurry <| setBallot interval)
    
    module VoteUtxo =
        
        let get (getUTXO : Outpoint -> DataAccess<OutputStatus>) set outpoint : DataAccess<OutputStatus> =
            match Map.tryFind outpoint set with
            | Some x -> ret x
            | None   -> getUTXO outpoint
        
        let getOutput (getUTXO : Outpoint -> DataAccess<OutputStatus>) set outpoint : DataAccess<Output> = 
            get getUTXO set outpoint
            |@> function
            | Unspent output -> output
            | NoOutput 
            | Spent _        -> failwith "Expected output to be unspent"
        
        let handleTransaction (getUTXO : Outpoint -> DataAccess<OutputStatus>) txHash tx set : DataAccess<Map<Outpoint, OutputStatus>> =
            
            let folder state input = dataAccess {
                let! output = getOutput getUTXO state input
                return Map.add input (Spent output) state
            }
            
            let outputsWithIndex = List.mapi (fun i output -> (uint32 i,output)) tx.outputs
            
            let set = dataAccess {
                let! set =
                    tx.inputs
                    |> List.choose (function | Outpoint outpoint -> Some outpoint | _ -> None)
                    |> foldM folder set
        
                return List.fold (fun state (index,output) ->
                    if Transaction.isOutputSpendable output then
                        let outpoint = {txHash=txHash;index=index;}
                        Map.add outpoint (Unspent output) state
                    else
                        state
                    ) set outputsWithIndex
            }
            
            set
        
        let private handleBlock
            (getUTXO : Outpoint -> DataAccess<OutputStatus>)
            (block : Block)
            (map : Map<Outpoint, OutputStatus>)
            : DataAccess<Map<Outpoint, OutputStatus>> =
                block.transactions
                |> foldM (fun map ex -> handleTransaction getUTXO ex.txHash ex.tx map ) map
        
        let private getUTXO interval (outpoint : Outpoint) : DataAccess<OutputStatus> =
            getVoteUtxo interval
            |@> Map.tryFind outpoint
            |@> Option.defaultValue NoOutput
        
        let update interval block =
            getVoteUtxo interval
            >>= handleBlock (getUTXO interval) block
            >>= VoteUtxo.put interval
    
    module Fund =
        
        let findPayoutTx (chainParams : ChainParameters) (transactions : Transaction list) : Transaction option =
        
            let cgpCW = function | ContractWitness cw -> cw.contractId = chainParams.cgpContractId | _ -> false
            
            transactions
            |> List.filter ( fun tx -> List.exists cgpCW tx.witnesses )
            |> List.tryHead
        
        let trySubtract (diff : uint64) (x : uint64) =
            option { if diff <= x then return x - diff }
        
        let withdraw fund spend = option {
            let! oldBalance = Map.tryFind spend.asset fund
            let! newBalance = trySubtract spend.amount oldBalance
            return Map.add spend.asset newBalance fund
        }
        
        let deposit fund spend =
            Map.tryFind spend.asset fund
            |> Option.defaultValue 0UL
            |> fun balance -> Some <| Map.add spend.asset (balance + spend.amount) fund
        
        // TODO: change when handling reorgs
        // Payout is deducted from the fund
        let deductPayout chainParams direction fund tx =
            
            let op = match direction with | Add -> withdraw | Undo -> deposit
            
            let deductableSpends {lock=lock; spend=spend} =
                match lock with
                | Contract cId when cId = chainParams.cgpContractId ->
                    None // Payout output to the CGP itself doesn't affect the fund.
                | PK _
                | Contract _ ->
                    Some spend
                | _ ->
                    None
            
            tx.outputs
            |> List.choose deductableSpends 
            |> FSharpx.Option.foldM op fund
        
        // Donations and coinbase allocations are added to the fund 
        let addIncome chainParams direction fund tx =
            
            let op = match direction with | Add -> deposit | Undo -> withdraw
            
            let incomingSpends {lock=lock; spend=spend} =
                match lock with
                | Contract cId when cId = chainParams.cgpContractId ->
                    Some spend
                | _ ->
                    None
            
            tx.outputs
            |> List.choose incomingSpends
            |> FSharpx.Option.foldM op fund
        
        let private getFund interval : DataAccess<Fund> =
            Fund.tryGet interval
            |@> Option.defaultValue Map.empty
        
        let update interval chainParams (block : Block) = dataAccess {
            let transactions =
                block.transactions
                |> List.map (fun ex -> ex.tx)
            
            let! initialFund = getFund interval
            
            //find payoutTx and remove from fund
            let fund = option {
                //add token to fund
                //TODO: Change it to get ONLY the snapshot balance of the contract
                let! fundAfterIncome =
                    transactions
                    |> FSharpx.Option.foldM (addIncome chainParams Add) initialFund
                    
                if CGP.isPayoutBlock chainParams block.header.blockNumber then
                    let! payoutTx =
                        transactions
                        |> findPayoutTx chainParams
                    return! deductPayout chainParams Add fundAfterIncome payoutTx
                else
                    return fundAfterIncome
            }
            
            return! fund
            |> Option.map (Fund.put interval)
            |> Option.defaultValue nothing
        }
    
    module Winner =
        
        let update interval chainParams = dataAccess {
            
            let! lastFund =
                if interval >= 1u then
                    Fund.tryGet (interval - 1u)
                    |@> Option.defaultValue Map.empty
                else
                    ret Map.empty
            
            let! lastAllocation =
                Allocation.tryGet interval
            
            let env : Env =
                {
                    coinbaseCorrectionCap =
                        chainParams.allocationCorrectionCap
                        |> Tally.allocationToCoinbaseRatio
                    
                    lowerCoinbaseBound =
                        chainParams.upperAllocationBound
                        |> Tally.allocationToCoinbaseRatio
                    
                    lastCoinbaseRatio =
                        lastAllocation
                        |> Option.defaultValue 0uy
                        |> Tally.allocationToCoinbaseRatio
                    
                    lastFund =
                        lastFund
                }
            
            let! balances          = PKBalance    .tryGet (interval - 1u) |@> Option.defaultValue Map.empty
            let! allocationBallots = PKAllocation .tryGet (interval - 1u) |@> Option.defaultValue Map.empty
            let! payoutBallots     = PKPayout     .tryGet (interval - 1u) |@> Option.defaultValue Map.empty
            
            let tally  = Tally.createTally env balances allocationBallots payoutBallots
            let winner = Tally.getWinner tally
            
            return! winner
            |> Option.map (Winner.put interval)
            |> Option.defaultValue nothing
        }
    
    let addBlock chainParams (block : Block) blockHash : DataAccess<unit> =
        let blockNumber   = block.header.blockNumber
        let interval      = CGP.getInterval          chainParams blockNumber
        let snapshot      = CGP.getSnapshotBlock     chainParams interval
        let endOfInterval = CGP.getLastIntervalBlock chainParams interval
        
        dataAccess {
            let! tip = Tip.get
            if tip.blockHash <> blockHash && block.header.blockNumber >= tip.blockNumber then
                if tip.blockHash <> block.header.parent then
                    failwithf "trying to add a block to the tip but the tip is in different chain %A %A" block.header.blockNumber tip.blockNumber
                if true then
                    do! Tip       .update block blockHash
                    do! VoteUtxo  .update interval block
                if blockNumber = snapshot then 
                    do! PKBalance .update interval chainParams
                    do! Fund      .update interval chainParams block
                if snapshot < blockNumber && blockNumber <= endOfInterval then
                    do! PKBallot  .update interval chainParams block
                if CGP.isTallyBlock chainParams blockNumber then
                    do! Winner    .update interval chainParams
        }

let sync dataAccess session chainParams tipBlockHash (tipHeader:BlockHeader) (getHeader:Hash -> BlockHeader) (getBlock:Hash -> Block) =
    let account = DataAccess.Tip.get dataAccess session
    // Find the fork block of the account and the blockchain, logging actions
    // to perform. Undo each block in the account's chain but not the blockchain,
    // and add each block in the blockchain but not the account's chain.
    let rec locate ((x,i),(y,j)) acc =

        if x = y && i = j then acc
        elif i > j
        then
            locate (((getHeader x).parent, i-1ul), (y,j)) ((Add, getBlock x, x) :: acc)
        elif i < j
        then
            locate ((x,i), ((getHeader y).parent, j-1ul)) ((Undo, getBlock y, y) :: acc)
        else
            locate (((getHeader x).parent, i-1ul), ((getHeader y).parent, j-1ul)) ((Add, getBlock x, x) :: (Undo, getBlock y,y) :: acc)

    let actions = locate ((tipBlockHash, tipHeader.blockNumber),
                          (account.blockHash, account.blockNumber)) []

    let toUndo, toAdd = List.partition (function | Undo, _, _ -> true | _ -> false) actions
    let toUndo = List.rev toUndo     // Blocks to be undo were found backwards.
    let sortedActions = toUndo @ toAdd

    sortedActions
    |> FreeDataAccess.iter (function
        //| (Undo, block, hash) -> undoBlock   chainParams hash block
        | (Add, block, hash) -> DA.addBlock chainParams block hash
        )
    |> FreeDataAccess.Compute.compute dataAccess session

let addBlock dataAccess session chainParams blockHash block =
    DA.addBlock chainParams block blockHash
    |> FreeDataAccess.Compute.compute dataAccess session

let init dataAccess session =
    DataAccess.Tip.put dataAccess session empty

let reset dataAccess session =
    DataAccess.Tip.put dataAccess session empty

    DataAccess.VoteUtxoSet.truncate dataAccess session
    DataAccess.Fund.truncate dataAccess session
    DataAccess.Winner.truncate dataAccess session
    DataAccess.PKPayout.truncate dataAccess session
    DataAccess.PKBalance.truncate dataAccess session
    DataAccess.PKAllocation.truncate dataAccess session
