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

module S = Serialization
module CryptoSig = Crypto.Signature

let empty =
    {
        blockHash = Hash.zero
        blockNumber = 0ul
    }

let option = FSharpx.Option.maybe

let private getBytes str = Encoding.Default.GetBytes (str : string)

let private getFinalBlockNumber (chainParams: ChainParameters) blockNumber =
    chainParams.intervalLength * CGP.getInterval chainParams blockNumber

let private getFund dataAccess session interval =
        match DataAccess.Fund.tryGet dataAccess session interval with
        | Some fund -> fund
        | None -> Map.empty

let private getVoteUtxo dataAccess session interval =
        VoteUtxoSet.tryGet dataAccess session interval
        |> Option.defaultValue
               (VoteUtxoSet.tryGet dataAccess session (interval - 1u)
                |> Option.defaultValue Map.empty)

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

let getUTXO dataAccess session interval outpoint =
    getVoteUtxo dataAccess session interval
    |> Map.tryFind outpoint
    |> Option.defaultValue NoOutput

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
let mapPkOutputs validMaturity (utxos: Map<Outpoint,OutputStatus>) =
    utxos
    |> Map.toList
    |> List.choose (function
            | _, Unspent output -> Some output
            | _ -> None )
    |> List.choose
           (function
            | {lock=PK pkHash; spend= {asset=asset;amount=amount}} ->
                Some (pkHash,asset,amount)
            | {lock= Coinbase (blockNumber,pkHash); spend= {asset=asset;amount=amount}} when validMaturity >= blockNumber ->
                Some (pkHash,asset,amount)
            | _ ->
                None)

let private addPKBalance dataAccess session interval chainParams =
    
    
    let pkBalance =
        PKBalance.tryGet dataAccess session interval
        |> Option.defaultValue
               (PKBalance.tryGet dataAccess session (interval - 1u)
                |> Option.defaultValue Map.empty)
    
    let getAmount address =
        pkBalance
        |> Map.tryFind address
        |> Option.defaultValue 0UL
        
    let validMaturity = (CGP.getSnapshotBlock chainParams interval) - chainParams.coinbaseMaturity
    
    let voteUtxo = 
        VoteUtxoSet.tryGet dataAccess session interval
        |> Option.defaultValue Map.empty
    
    voteUtxo
    |> mapPkOutputs validMaturity
    |> List.iter (fun (address,_,amount) ->
        PKBalance.tryGet dataAccess session interval
        |> Option.defaultValue Map.empty
        |> Map.add address ((getAmount address) + amount)
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

let getValidBallot chainParam blockNumber command  =
        let readCommand = S.Ballot.deserialize command
        
        let interval = CGP.getInterval chainParam blockNumber
        
        let serialize data = Data.serialize data |> FsBech32.Base16.encode
        
        let ballotId =
            command
            |> FsBech32.Base16.encode
            |> ZFStar.fsToFstString
        
        let serBallot = serialize (String ballotId) 
        
        let serInterval = serialize (U32 interval) 
        
        let concat = String.concat "" [serInterval; serBallot]
        
        let hashSerializeData = Hash.compute (getBytes concat)
        
        readCommand, hashSerializeData
    
let getValidSignatures (sigs:Map<Prims.string,data>) (ballot:Ballot option) verifyMessage =
    option {
        let! ballot = ballot
        let! verifyMessage = verifyMessage
        
        let fromSigs =
            sigs
            |> Map.toList
            |> List.choose (fun (pkString,data) ->
                let signature =
                    match data with
                    | Signature sigs ->
                        Some (Crypto.Signature sigs)
                    | _ ->
                        None
                let publicKey = PublicKey.fromString (ZFStar.fstToFsString pkString)

                publicKey |> Option.map (fun pk -> (pk, signature)) 
                )
            |> Map.ofList
        let writeSigs (pk,signature) =
            match signature with
            | Some signature ->
                match Crypto.verify pk signature verifyMessage with
                | Valid ->
                    Some (pk,signature)
                | Invalid ->
                    None
            | _ ->
                None

        let validSigs =
            fromSigs
            |> Map.toSeq
            |> Seq.choose writeSigs
            |> Map.ofSeq

        return (ballot, validSigs)
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
    let findCommand command map =
         match Map.tryFind (ZFStar.fsToFstString command) map with
                    | Some (String commands) ->
                        let encoded = (ZFStar.fstToFsString commands)
                                      |> FsBech32.Base16.decode
                                      |> Option.defaultValue ""B
                        match getValidBallot chainParams blockNumber encoded with
                        | Some (Allocation allocation),message when command = "Allocation" ->
                            Some (Allocation allocation), Some message
                        | Some (Payout (recipient,spends)),message when command = "Payout" ->
                            Some (Payout (recipient,spends)),Some message
                        | _ -> None, None 
                    | _ -> None, None
                    
    let findSignature command map ballot message =
        match Map.tryFind (ZFStar.fsToFstString command) map with
                | Some (Collection (Dict (signatures,_))) ->
                    match getValidSignatures signatures ballot message with
                    |Some (Allocation allocation,sigs) ->
                        setAllocation allocation sigs
                    |Some (Payout (recipient,spends),sigs) ->
                        setPayout (recipient,spends) sigs
                    | _ -> ()
                | _ -> ()
    
    let analize messageBody command =
        messageBody
        |> Option.iter (
            function
            | Collection (Dict (map,_)) ->
                let ballot, message = 
                   findCommand command map
                findSignature "Signature" map ballot message
            | _ -> ())

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
        printfn "interval:%A" interval
        //get from data the current fund
        let fund = getFund dataAccess session interval
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
        if block.header.blockNumber > CGP.getSnapshotBlock chainParams interval && block.header.blockNumber <= getFinalBlockNumber chainParams block.header.blockNumber then
            transactions
            |> findVotes dataAccess session interval chainParams block.header.blockNumber

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
