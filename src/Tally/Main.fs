module Tally.Main

open DataAccess
open Infrastructure
open Messaging.Services
open Messaging.Events
open ServiceBus.Agent
open Consensus
open Consensus.Chain
open Types
open Messaging.Services.Tally
open Logary.Message
open Tally

let private getFirstBlockNumber chainParams interval =
    (chainParams * (interval-1u))

// I'm terribly sorry about that...
let mutable private MUT_reset_trigger = false
let private setTrigger()   = MUT_reset_trigger <- true
let private resetTrigger() = MUT_reset_trigger <- false
let private isTriggered() = MUT_reset_trigger

let private syncInterval (chainParams:ChainParameters) dataAccess session client interval =
    match Blockchain.getBlockByNumber client (getFirstBlockNumber chainParams.intervalLength interval) with
    | Some {header=blockHeader} ->
        let blockHash = Block.hash blockHeader
        Repository.sync dataAccess session chainParams blockHash blockHeader (Blockchain.getBlockHeader client >> Option.get) (Blockchain.getBlock client true >> Option.get)

        eventX "Tally synced to block #{blockNumber} {blockHash}"
        >> setField "blockNumber" blockHeader.blockNumber
        >> setField "blockHash" (Hash.toString blockHash)
        |> Log.info
       
        View.show dataAccess session chainParams interval View.empty
    | None ->
        View.empty

let commandHandler (chainParams:ChainParameters) client command dataAccess session view =
    match command with
    | Resync ->
        Repository.reset dataAccess session
        syncInterval chainParams dataAccess session client 630u

    | Tally interval ->
        View.show dataAccess session chainParams interval view 

let tallyUpdate chainParams dataAccess session client block =
    if CGP.isTallyBlock chainParams block.header.blockNumber then
        resetTrigger()
        syncInterval chainParams dataAccess session client (CGP.getInterval chainParams block.header.blockNumber)
    else
        View.empty
        
let setWinner chainParams dataAccess session client block =
    let blockNumber = block.header.blockNumber
    if CGP.isPayoutBlock chainParams blockNumber then
        let interval =
            CGP.getInterval chainParams blockNumber
        let view = syncInterval chainParams dataAccess session client (CGP.getInterval chainParams blockNumber)
        let winner = View.getWinner dataAccess session interval view
        Tally.setCGP client winner 

let eventHandler chainParams client event dataAccess session view =
    match event with
    | BlockAdded (_,block) ->
        setWinner chainParams dataAccess session client block
        tallyUpdate chainParams dataAccess session client block
    | BlockRemoved (_,block) ->
        let blockNumber = block.header.blockNumber
        if CGP.isTallyBlock chainParams (blockNumber + 1u) then
            setTrigger()
        elif CGP.isLastIntervalBlock chainParams blockNumber && isTriggered() then
            resetTrigger()
            Repository.resetInterval dataAccess session chainParams block
        setWinner chainParams dataAccess session client block
        tallyUpdate chainParams dataAccess session client block
    | _ ->
        view

let private reply<'a> (requestId:RequestId) (value : Result<'a,string>) =
    requestId.reply value

let requestHandler chainParams (requestId:RequestId) request dataAccess session (view:View.T) =
    match request with
    | GetWinner interval ->
        let view = View.show dataAccess session chainParams interval view 
        Ok view.winner
        |> reply<Option<Winner>> requestId
    |> ignore
    
    view

type Wipe =
    | Full
    | Reset
    | NoWipe

let main dataPath busName chain (wipe:Wipe) =
    let dataPath = Platform.combine dataPath "tally"

    if wipe = Full then
        eventX "Wiping Tally database"
        |> Log.info
        
        if System.IO.Directory.Exists dataPath then
            System.IO.Directory.Delete (dataPath,true)

    Actor.create<Command,Request,Event, View.T> busName serviceName (fun _ sbObservable ebObservable ->
        let databaseContext = DataAccess.createContext dataPath
        let dataAccess = DataAccess.init databaseContext

        let client = ServiceBus.Client.create busName
        use session = DatabaseContext.createSession databaseContext

        match DataAccess.Tip.tryGet dataAccess session with 
        | None -> 
            eventX "Creating Tally"
            |> Log.info
            
            Repository.init dataAccess session
        | Some _ -> ()
            
        if wipe = Reset then
            eventX "Resetting Tally"
            |> Log.info
            
            Repository.reset dataAccess session
         
        eventX "Setting Tally"
        |> Log.info    
        let chainParams = Chain.getChainParameters chain
        
        let blockNumber =
            DataAccess.Tip.tryGet dataAccess session
            |> Option.map (fun tip -> tip.blockNumber)
            |> Option.defaultValue 1ul
        
        let interval = CGP.getInterval chainParams blockNumber
        let view = View.show dataAccess session chainParams interval View.empty
        
        Session.commit session

        let sbObservable =
            sbObservable
            |> Observable.map (function
                | ServiceBus.Agent.Request (requestId, r) -> requestHandler chainParams requestId r
                | ServiceBus.Agent.Command command -> commandHandler chainParams client command
            )
                
        let ebObservable =
            ebObservable
            |> Observable.map (eventHandler chainParams client)

        let observable =
            Observable.merge sbObservable ebObservable
            |> Observable.scan (fun view handler ->
                use session = DatabaseContext.createSession databaseContext
                let view = handler dataAccess session view

                Session.commit session
                
                view
            ) view

        Disposables.fromFunction (fun () ->
            DataAccess.dispose dataAccess
            Disposables.dispose databaseContext), observable
    )
