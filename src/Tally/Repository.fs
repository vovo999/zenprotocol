module Tally.Repository

open Consensus
open Consensus
open Consensus.Chain
open Consensus.Crypto
open UtxoSet
open Types
open Hash
open Logary.Message
open Infrastructure
open Messaging.Services
open Tally
open DataAccess
open Tally.Serialization
open Tally.View
open Wallet
open Zen.Types.Data

let empty = 
    {
        blockHash = Hash.zero
        blockNumber = 0ul
    }

let private getFund dataAccess session interval =
        match DataAccess.Fund.tryGet dataAccess session interval with
        | Some fund -> fund
        | None -> Map.empty

//let private getVoteUtxo dataAccess session interval =
//        match DataAccess.VoteUtxoSet.tryGet dataAccess session interval with
//        | Some voteUtxo ->
//            voteUtxo
//        | None -> Map.empty

//let getOutputs dataAccess session view mode addresses : PointedOutput list =
//    DataAccess.AddressOutpoints.get view dataAccess session addresses
//    |> DataAccess.OutpointOutputs.get view dataAccess session
//    |> List.choose (fun dbOutput -> 
//        if mode <> UnspentOnly || dbOutput.status = Unspent then 
//            Some (dbOutput.outpoint, { spend = dbOutput.spend; lock = dbOutput.lock })
//        else
//            None)
//
//let getBalance dataAccess session view mode addresses =
//    getOutputs dataAccess session view mode addresses 
//    |> List.fold (fun balance (_,output) ->
//        match Map.tryFind output.spend.asset balance with
//        | Some amount -> Map.add output.spend.asset (amount + output.spend.amount) balance
//        | None -> Map.add output.spend.asset output.spend.amount balance
//    ) Map.empty
        
//let getUTXO dataAccess session interval outpoint =
//    getVoteUtxo dataAccess session interval
//    |> Map.tryFind outpoint
//    |> Option.defaultValue NoOutput

let private findPayoutTx chainParams transactions =
    transactions
    |> List.map (fun (tx, _) -> tx )
    |> List.tryFind (fun tx ->
        tx.witnesses
        |> List.filter (fun witness ->
            match witness with
            | ContractWitness cw when cw.contractId = chainParams.cgpContractId -> true
            | _ -> false)
        |> List.isEmpty
        |> not)
    



let private setFund total spend fund =
    if total <= 0UL then
        Map.remove spend.asset fund
    else
        Map.add spend.asset total fund
        

let private handlePayout dataAccess session interval chainParams payoutTx adding fund =
    let op = if adding then (+) else (-)
    payoutTx.outputs
    |> List.iter (function
        |{lock=Contract cId;spend=_} when cId = chainParams.cgpContractId ->
            ()
        |{lock=PK _; spend=spend}
        |{lock=Contract _; spend=spend} ->
            match Map.tryFind spend.asset fund with
                | Some amount ->
                    setFund (op amount spend.amount) spend fund
                | None -> fund
            |> Fund.put dataAccess session interval
        | _ ->
            ())

let private handleFund dataAccess session interval adding output chainParams (fund:Balance) =
    let op = if adding then (+) else (-)
    match output.lock with
            | Contract cId when cId = chainParams.cgpContractId ->
                match Map.tryFind output.spend.asset fund with
                    | Some amount ->
                            let total = op amount output.spend.amount
                            setFund total output.spend fund
                    | None -> fund
                |> Fund.put dataAccess session interval
            | _ ->
                ()
let private addPKBalance dataAccess session interval output =
    ()

module CryptoSignature = Crypto.Signature
module CryptoPublicKey = Crypto.PublicKey

let addMap dataAccess session interval pk ballot :PKBallot =
    let map = 
        PKBallot.tryGet dataAccess session interval
        |> Option.defaultValue Map.empty
        
    match Map.tryFind pk map with
    | Some _ -> map 
    | None -> Map.add pk ballot map
    

let foo dataAccess session interval arr command =
    FSharpx.Option.maybe {
        let readItem valueFn s = Serialization.Serialization.String.read s, Serialization.Serialization.Array.read valueFn s
        let! map = Serialization.Serialization.Map.deserialize (readItem Serialization.Serialization.Byte.read) arr
        let! ballotEnc = map.TryFind command
        let! sigsEnc = map.TryFind "Sigs"
        let! ballot = Serialization.Serialization.Ballot.deserialize ballotEnc
        let sigDeserialize s = Serialization.Serialization.Array.read (Serialization.Serialization.Byte.read) s |> CryptoSignature.deserialize
        let pkDeserialize s = Serialization.Serialization.Array.read (Serialization.Serialization.Byte.read) s |> CryptoPublicKey.deserialize
        let sigsItemDeserialize s = pkDeserialize s, sigDeserialize s
        let! sigs = Serialization.Serialization.Map.deserialize sigsItemDeserialize sigsEnc
        let sigs =
            sigs
            |> Map.iter (fun pk signature ->
                match pk, signature with
                | Some pk, Some signature ->
                    match Crypto.verify pk signature (Hash.compute ballotEnc) with
                    | VerifyResult.Valid ->
                        addMap dataAccess session interval pk ballot
                        |> PKBallot.put dataAccess session interval 
                    | VerifyResult.Invalid ->
                        ()
                | _ -> ())
            
        return (ballot, sigs)
    }

let findVotes dataAccess session interval chainParams transactions =
    transactions
    |> List.iter (fun (tx, _)  ->
        //let x =
            tx.witnesses
            |> List.filter (fun witness ->
                match witness with
                | ContractWitness cw when cw.contractId = chainParams.votingContractId -> true
                | _ -> false)
            |> List.map (fun witness ->
                match witness with
                | ContractWitness cw ->
                    cw.messageBody
                    |> Option.bind (
                        function
                        | ByteArray arr ->
                            foo dataAccess session interval arr cw.command
                        | _ -> failwith "")
                    
                | _ -> failwith ""  ) |> ignore 
            
            ()
        
    )
            
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
let undoVoteUtxoBlock getUTXO block utxoSet =
    let handleInput utxoSet input =
        match UtxoSet.get getUTXO utxoSet input with
        | Spent output ->
            Map.add input (Unspent output) utxoSet
        | NoOutput ->
            utxoSet
        | Unspent _ -> failwith "Expected output to be spent"
    
    let handleOutput ex utxoSet (i,output) =
        if Transaction.isOutputSpendable output then
            let outpoint = {txHash=ex.txHash; index=uint32 i}
            Map.add outpoint NoOutput utxoSet
        else
            utxoSet
    
    let handleTx (ex:TransactionExtended) utxoSet =        
        let utxoSet =
            ex.tx.inputs
            |> List.choose (function | Outpoint outpoint -> Some outpoint | Mint _ -> None)
            |> List.fold handleInput utxoSet

        ex.tx.outputs
        |> List.mapi (fun i output -> i,output )
        |> List.fold (handleOutput ex) utxoSet
    
    // remove all outputs
    List.foldBack handleTx block.transactions utxoSet

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
                
let private handleBlock getUTXO block voteUtxo =
    block.transactions
    |> List.fold (fun map ex -> handleTransaction getUTXO ex.txHash ex.tx map ) voteUtxo
    
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
    
    //add balance to db the PKHashs balance is a Map<asset,amount> => Map<PKHASH,Map<Asset,Amount>>
    // MAP<PK, Ballot>
    
    //CW with CID with contract of Voting

    
        let interval = CGP.getInterval chainParams block.header.blockNumber
        
        //get from data the current fund
        let fund = getFund dataAccess session interval
        
        eventX "Tally adding block #{blockNumber} for interval #{interval}"
        >> setField "blockNumber" block.header .blockNumber
        >> setField "interval" interval
        |> Log.info
        
        let transactions =
            block.transactions
            |> List.map (fun ex -> ex.tx, ex.txHash)
            
        transactions
        |> List.iter (fun (tx,_) ->
            tx.outputs
            |> List.iter (fun output -> addPKBalance dataAccess session interval output ))
            
        transactions
        |> findVotes dataAccess session interval chainParams
            //|> List.iter (fun witness -> addPKBallot dataAccess session interval output chainParams ))
        
//        printfn "intervalADDBLOCK %A" interval
//        getVoteUtxo dataAccess session interval
//        |> handleBlock (getUTXO dataAccess session interval) block
//        |> VoteUtxoSet.put dataAccess session interval
        
        //add token to fund
        transactions
        |> List.iter (fun (tx,_) ->
            tx.outputs
            |> List.iter (fun output -> handleFund dataAccess session interval true output chainParams fund)
        )
        //find payoutTx and remove from fund
        if CGP.isPayoutBlock chainParams block.header.blockNumber then
            transactions
            |> findPayoutTx chainParams
            |> function
                |Some payoutTx ->
                    handlePayout dataAccess session interval chainParams payoutTx false fund
                |None ->
                    ()
        
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
    
//    let voteUtxo = getVoteUtxo dataAccess session interval
//    
    let fund = getFund dataAccess session interval
//    
//    //undo VoteUtxo
//    voteUtxo
//    |> undoVoteUtxoBlock (getUTXO dataAccess session interval) block
//    |> VoteUtxoSet.put dataAccess session interval
    
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
        |> List.map (fun ex -> ex.tx, ex.txHash)
        |> findPayoutTx chainParams
        |> function
            |None ->
                ()
            |Some payoutTx ->
                handlePayout dataAccess session interval chainParams payoutTx true fund
                
                
    
    {account with blockNumber = block.header.blockNumber - 1ul; blockHash = block.header.parent}
    |> DataAccess.Tip.put dataAccess session

type private SyncAction =
    | Undo of Block * Hash
    | Add of Block * Hash

let sync dataAccess session chainParams tipBlockHash (tipHeader:BlockHeader) (getHeader:Hash -> BlockHeader) (getBlock:Hash -> Block) =
    let account = DataAccess.Tip.get dataAccess session
//    eprintfn "000000000000000 %A" tipHeader.blockNumber
//    let getHeader =
//        getHeader >> Option.get
//    eprintfn "1111111111111111 %A" tipHeader.blockNumber
//    let getBlock =
//        getBlock >> Option.get
//    eprintfn "22222222222222222 %A" tipHeader.blockNumber

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

//    DataAccess.VoteUtxoSet.truncate dataAccess session
    
    
