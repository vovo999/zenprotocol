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
open FSharpx.Option

module S = Serialization
module CryptoSig = Crypto.Signature

let empty =
    {
        blockHash   = Hash.zero
        blockNumber = 0ul
    }

let option = FSharpx.Option.maybe

let private lastIntervalBlock (chainParams: ChainParameters) blockNumber =
    chainParams.intervalLength * CGP.getInterval chainParams blockNumber

let private getFund dataAccess session interval =
    match DataAccess.Fund.tryGet dataAccess session interval with
    | Some fund -> fund
    | None -> Map.empty

let private getVoteUtxo dataAccess session interval =
    VoteUtxoSet.tryGet dataAccess session interval
    |> Option.defaultWith (fun () ->
        VoteUtxoSet.tryGet dataAccess session (interval - 1u)
        |> Option.defaultValue Map.empty)

let getUTXO dataAccess session interval outpoint =
    getVoteUtxo dataAccess session interval
    |> Map.tryFind outpoint
    |> Option.defaultValue NoOutput

let private findPayoutTx (chainParams : ChainParameters) (transactions : Transaction list) : Transaction option =
    
    let cgpCW = function | ContractWitness cw -> cw.contractId = chainParams.cgpContractId | _ -> false
    
    transactions
    |> List.filter ( fun tx -> List.exists cgpCW tx.witnesses )
    |> List.tryHead

let private setFund total spend fund =
    if total = 0UL then
        Map.remove spend.asset fund
    else
        Map.add spend.asset total fund

// Payout is deducted from the fund
let private handlePayout dataAccess session interval chainParams payoutTx adding fund =
    let op = if adding then (+) else (-)
    payoutTx.outputs
    |> List.iter (function
        | {lock=Contract cId; spend=_} when cId = chainParams.cgpContractId ->
            () // Payout output to the CGP itself doesn't affect the fund.
        | {lock=PK _; spend=spend}
        | {lock=Contract _; spend=spend} ->
            match Map.tryFind spend.asset fund with
                | Some amount ->
                    setFund (op amount spend.amount) spend fund
                | None -> failwithf "there should not be this tx" //TODO: change when handling the reorgs
            |> Fund.put dataAccess session interval
        | _ ->
            ())

// Donations and coinbase allocations are added to the fund 
let private handleFund dataAccess session interval adding output chainParams (fund:Fund) =
    let op = if adding then (+) else (-)
    match output.lock with
            | Contract cId when cId = chainParams.cgpContractId ->
                match Map.tryFind output.spend.asset fund with
                    | Some amount ->
                            let total = op amount output.spend.amount
                            setFund total output.spend fund
                    | None -> setFund output.spend.amount output.spend fund 
                |> Fund.put dataAccess session interval
            | _ ->
                ()

let private getUnspentOutputs : Outpoint * OutputStatus -> Output option =
    function
    | _, Unspent output -> Some output
    | _                 -> None

let getSpendablePKs validMaturity (utxos: Map<Outpoint,OutputStatus>) =
    utxos
    |> Map.toList
    |> List.choose getUnspentOutputs
    |> List.choose
           (function
            | {lock=PK pkHash; spend={asset=asset;amount=amount}} when asset = Asset.Zen ->
                Some (pkHash,amount)
            | {lock=Coinbase (blockNumber,pkHash); spend={asset=asset;amount=amount}} when asset = Asset.Zen && validMaturity >= blockNumber ->
                Some (pkHash,amount)
            | _ ->
                None
            )

let private addPKBalance dataAccess session interval chainParams =
    let getAmount address =
        PKBalance.tryGet dataAccess session interval
        |> Option.defaultValue Map.empty
        |> Map.tryFind address
        |> Option.defaultValue 0UL

    // Used to check that the coinbase tx are considered as unspent and unspendable beacuse of their maturity
    let validMaturity = CGP.getSnapshotBlock chainParams interval - chainParams.coinbaseMaturity
    
    getVoteUtxo dataAccess session interval
    |> getSpendablePKs validMaturity
    |> List.iter (fun (pkHash, amount) ->
        PKBalance.tryGet dataAccess session interval
        |> Option.defaultValue Map.empty
        |> Map.add pkHash ((getAmount pkHash) + amount)
        |> PKBalance.put dataAccess session interval)

let addAllocation dataAccess session interval pk allocation =
    let map =
        PKAllocation.tryGet dataAccess session interval
        |> Option.defaultValue Map.empty

    match Map.tryFind pk map with
    | Some _ -> map
    | None -> Map.add pk allocation map

let addPayout dataAccess session interval pk payout =
    let map =
        PKPayout.tryGet dataAccess session interval
        |> Option.defaultValue Map.empty

    match Map.tryFind pk map with
    | Some _ -> map
    | None -> Map.add pk payout map

let getValidBallot chainParam blockNumber ballotId  =
        
        let interval = CGP.getInterval chainParam blockNumber
        
        let serialize data = Data.serialize data |> FsBech32.Base16.encode
        
        let serBallot = serialize (String ballotId) 
        
        let serInterval = serialize (U32 interval) 
        
        let concat = String.concat "" [serInterval; serBallot]
        
        let hashSerializeData = Hash.compute (Encoding.Default.GetBytes concat)
        
        hashSerializeData
    
let getValidSignatures (sigs : Map<Prims.string,data>) verifyMessage =
    option {
        
        let extractPkSig (pkString, data) = option {
                
                let! publicKey =
                    pkString
                    |> ZFStar.fstToFsString
                    |> PublicKey.fromString
                
                let! signature =
                    match data with
                    | Signature sign ->
                        Some (ZFStar.fstToFsSignature sign)
                    | _ ->
                        None
                
                return (publicKey, signature) 
        }
        
        let verifySig (pk,signature) =
            match Crypto.verify pk signature verifyMessage with
            | Valid ->
                Some (pk, signature)
            | Invalid ->
                None
        
        let validSigs =
            sigs
            |> Map.toSeq
            |> Seq.choose (extractPkSig >=> verifySig)
            |> Map.ofSeq

        return validSigs
    }

let findVotes dataAccess session interval chainParams blockNumber transactions =
    
    let setAllocation allocation sigs =
        sigs
        |> Map.iter (fun pk _ ->
            addAllocation dataAccess session interval pk allocation
            |> PKAllocation.put dataAccess session interval)
        
    let setPayout payout sigs =
        sigs
        |> Map.iter (fun pk _ ->
            addPayout dataAccess session interval pk payout
            |> PKPayout.put dataAccess session interval)
    
    let extractBallot command encBallot = option {
        
        let serBallot =
            encBallot
            |> ZFStar.fstToFsString
            |> FsBech32.Base16.decode
            |> Option.defaultValue ""B
        
        match! S.Ballot.deserialize serBallot with
        | Allocation allocation      when command = "Allocation" ->
            return Allocation allocation
        | Payout (recipient, spends) when command = "Payout"     ->
            return Payout (recipient, spends)
        | _ ->
            return! None
    }
    
    let findSignature command dataMap message = 
        dataMap
        |> Map.tryFind (ZFStar.fsToFstString command) 
        >>= function
        | Collection (Dict (signatures,_)) ->
            getValidSignatures signatures message
        | _ ->
            None
    
    let setBallot =
        function
        | Allocation allocation ->
            setAllocation allocation
        | Payout (recipient, spends) ->
            setPayout (recipient,spends)
    
    let analize messageBody command =
        option {
            let! map =
                match messageBody with
                | Some (Collection (Dict (map,_))) ->
                    Some map
                | _ ->
                    None
            let! data      = Map.tryFind (ZFStar.fsToFstString command) map
            let! encBallot = match data with | String cmd -> Some cmd | _ -> None
            let! ballot    = extractBallot command encBallot
            let  hash      = getValidBallot chainParams blockNumber encBallot
            let! sigMap    = findSignature "Signature" map hash
            setBallot ballot sigMap
        } |> ignore
    
    transactions
    |> List.iter (fun (tx, _)  ->
        tx.witnesses
        |> List.iter (
            function
            | ContractWitness {contractId=contractId; command=command; messageBody=messageBody} ->
                if contractId = chainParams.votingContractId then
                    analize messageBody command
            | _ -> ()))

let getOutput getUTXO state input =
    UtxoSet.get getUTXO state input
    |> function
    | Unspent output ->
        Some output
    | NoOutput ->
        None
    | Spent _  ->
        failwith "Expected output to be unspent"

//Duplicate/edited from Consensus.Utxo 
let private handleTransaction getUTXO txHash tx set =
    let folder state input =
        getOutput getUTXO state input
        |> Option.map (fun output -> 
            Map.add input (Spent <| output) state)
        |> Option.defaultValue state 

    let outputsWithIndex = List.mapi (fun i output -> (uint32 i,output)) tx.outputs

    let set =
        let set =
            tx.inputs
            |> List.choose (function | Outpoint outpoint -> Some outpoint | _ -> None)
            |> List.fold folder set

        List.fold (fun state (index,output) ->
            if Transaction.isOutputSpendable output then
                let outpoint = {txHash=txHash;index=index;}
                Map.add outpoint (Unspent output) state
            else
                state
            ) set outputsWithIndex

    set

let private handleBlock getUTXO block map =
    block.transactions
    |> List.fold (fun map ex -> handleTransaction getUTXO ex.txHash ex.tx map ) map

let getViewFund dataAccess session view =
    View.Fund.get dataAccess session view

let addBlock dataAccess session (chainParams:ChainParameters) blockHash block =
    let account = DataAccess.Tip.get dataAccess session
    if account.blockHash = blockHash || block.header.blockNumber < account.blockNumber then
        // we already handled the block, skip
        ()
    elif account.blockHash <> block.header.parent then
        failwithf "trying to add a block to account but account in different chain %A %A" (block.header.blockNumber) (account.blockNumber)
    else
        let blockNumber = block.header.blockNumber
        let interval = CGP.getInterval chainParams blockNumber

        //get from data the current fund
        #if DEBUG
        
        #else
        eventX "Tally adding block #{blockNumber} for interval #{interval}"
        >> setField "blockNumber" block.header .blockNumber
        >> setField "interval" interval
        |> Log.info
        #endif

        let transactions =
            block.transactions
            |> List.map (fun ex -> ex.tx, ex.txHash)
        
        getVoteUtxo dataAccess session interval
        |> handleBlock (getUTXO dataAccess session interval) block
        |> VoteUtxoSet.put dataAccess session interval
        
        // map of PKHash to Balance or to Snapshot Balance
        if blockNumber <= CGP.getSnapshotBlock chainParams interval then 
           addPKBalance dataAccess session interval chainParams
                      
        // map of PK to Ballots
        if block.header.blockNumber > CGP.getSnapshotBlock chainParams interval && block.header.blockNumber <= lastIntervalBlock chainParams block.header.blockNumber then
            transactions
            |> findVotes dataAccess session interval chainParams block.header.blockNumber

        //add token to fund
        //TODO: Change it to get ONLY the snapshot balance of the contract
        transactions
        |> List.iter (fun (tx,_) ->
            tx.outputs
            |> List.iter (fun output -> handleFund dataAccess session interval true output chainParams (getFund dataAccess session interval))
        )
        //find payoutTx and remove from fund
        //TODO: Change findPayoutTx
        if CGP.isPayoutBlock chainParams block.header.blockNumber then
            transactions
            |> List.map fst
            |> findPayoutTx chainParams
            |> Option.map (fun payoutTx -> handlePayout dataAccess session interval chainParams payoutTx false (getFund dataAccess session interval))
            |> Option.defaultValue ()
        
        { account with blockNumber = block.header.blockNumber; blockHash = blockHash}
        |> DataAccess.Tip.put dataAccess session

let undoBlock dataAccess session chainParams blockHash block =
    let account = DataAccess.Tip.get dataAccess session

    if account.blockHash = block.header.parent || block.header.blockNumber > account.blockNumber then
        // we already undo this block, skipping
        ()
    elif account.blockHash <> blockHash then
        failwithf "trying to undo a block to account but account in different chain %A %A" (block.header.blockNumber) (account.blockNumber)
    else

        let interval = CGP.getInterval chainParams block.header.blockNumber
    
        let fund = getFund dataAccess session interval
    
        //undo fund
        block.transactions
        |> List.rev
        |> List.iter (fun ex ->
            ex.tx.outputs
            |> List.iter (fun output ->
                handleFund dataAccess session interval false output chainParams fund)
            )
    
        //undo payout tx
        if CGP.isPayoutBlock chainParams block.header.blockNumber then
            block.transactions
            |> List.map (fun ex -> ex.tx)
            |> findPayoutTx chainParams
            |> Option.map (fun payoutTx -> handlePayout dataAccess session interval chainParams payoutTx true fund)
            |> Option.defaultValue ()
    
        {account with blockNumber = block.header.blockNumber - 1ul; blockHash = block.header.parent}
        |> DataAccess.Tip.put dataAccess session

type private SyncAction =
    | Undo of Block * Hash
    | Add of Block * Hash

let sync dataAccess session chainParams tipBlockHash (tipHeader:BlockHeader) (getHeader:Hash -> BlockHeader) (getBlock:Hash -> Block) =
    let account = DataAccess.Tip.get dataAccess session
    // Find the fork block of the account and the blockchain, logging actions
    // to perform. Undo each block in the account's chain but not the blockchain,
    // and add each block in the blockchain but not the account's chain.
    let rec locate ((x,i),(y,j)) acc =

        if x = y && i = j then acc
        elif i > j
        then
            locate (((getHeader x).parent, i-1ul), (y,j)) (Add (getBlock x, x) :: acc)
        elif i < j
        then
            locate ((x,i), ((getHeader y).parent, j-1ul)) (Undo (getBlock y, y) :: acc)
        else
            locate (((getHeader x).parent, i-1ul), ((getHeader y).parent, j-1ul)) (Add (getBlock x, x) :: Undo (getBlock y,y) :: acc)

    let actions = locate ((tipBlockHash, tipHeader.blockNumber),
                          (account.blockHash, account.blockNumber)) []

    let toUndo, toAdd = List.partition (function | Undo _ -> true | _ -> false) actions
    let toUndo = List.rev toUndo     // Blocks to be undo were found backwards.
    let sortedActions = toUndo @ toAdd

    sortedActions
    |> List.iter (function
        | Undo (block,hash) -> undoBlock dataAccess session chainParams hash block
        | Add (block,hash) -> addBlock dataAccess session chainParams hash block)

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
