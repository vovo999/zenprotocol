module Tally.Tests.TallyActorTests

open Consensus.Types
open Consensus
open NUnit.Framework
open Infrastructure.ServiceBus
open Blockchain
open Tally

module Services = Messaging.Services

//open Messaging.Services
//open FsNetMQ

let busName = "test"
let timestamp = 1515594186383UL + 1UL
let difficulty = 0x20fffffful

[<Test>]
let ``The dormant actor activates when given the signal to start the tally`` () =
     
     let header =
          {
               version     = Version0
               parent      = Hash.zero
               blockNumber = 15ul
               difficulty  = 0x20fffffful;
               commitments = Hash.zero
               timestamp   = timestamp
               nonce       = 0UL,0UL
          }
     
     let block =
          {
               header                      = header;
               transactions                = [];
               commitments                 = [];
               txMerkleRoot                = Hash.zero;
               witnessMerkleRoot           = Hash.zero;
               activeContractSetMerkleRoot = Hash.zero;
          }
     
     ()
     //let chain = Chain.Test
     //let chainParams = Chain.getChainParameters Chain.Test
     //let dataPath = Infrastructure.Platform.combine "./data" "test"
     //use client = Client.create busName
     //let databaseContext = Tally.DataAccess.createContext dataPath
     //let dataAccess = Tally.DataAccess.init databaseContext
     //
     //let client = Client.create busName
     //use session = DatabaseContext.createSession databaseContext
     //
     //use view = Tally.Main.eventHandler chainParams client event dataAccess session view
     //use actor = Tally.Main.main dataPath busName chain Tally.Main.Wipe.NoWipe
     //     
     //
     //EffectsWriter.publish (Messaging.Events.BlockAdded (Block.hash block.header, block))
     //|> ignore

let ``A vote is only registered if VotingContract has commands "Payout" or "Allocation"`` () =
     


