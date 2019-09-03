module Tally.Tests.RepositoryTests

open Blockchain.Serialization
open NUnit.Framework
open Tally
open DataAccess
open System.IO
open Infrastructure
open Consensus
open Consensus.Serialization.Serialization
open Tally
open Types
open Tally.Tests.VotingContractCode

module ZRTypes = Zen.Types.Realized
module ZETypes = Zen.Types.Extracted
module ZData = Zen.Types.Data
module CryptoPublicKey = Crypto.PublicKey

let difficulty = 0x20fffffful

let rlimit = 8000000u

let result = new Infrastructure.Result.ResultBuilder<string>()
let chainParams = Chain.testParameters

let votingContractId = chainParams.votingContractId

let payoutTx1 =
    Transaction.fromHex "000000000101d7de65a96e830d3b86e65b31724486750950aca121bad967b2efe8d99febfb5a0002022017ba8c88eb030b31a0da51ec5fe7bbd08ff807943f57b4cc4a33de4aca37fca6007e0000012a05f36501000000010002016203032229522443cf166e28468c58a4719ce01eb2d9b5b656ecae6e959001bbe8c469d6b5fe2beae7319d1bfc23297e149f4a1bdd2257ac6542336f98ac30c4ed79d94be7492e3d1251166b4f65c364a07a3682ebb7aa5599b84ac25b8a66c4f204af02822200abbf8805a203197e4ad548e4eaa2b16f683c013e31d316f387ecf7adc65b3fb2065061796f7574010c02065061796f7574064c30323031333434646333343366306163366430643164356436653633383861396463343935666632333062363530353635343535663034306334616264353635633164333031303030303739095369676e61747572650c024230333232323935323234343363663136366532383436386335386134373139636530316562326439623562363536656361653665393539303031626265386334363909c97a583847355e9057dc1fbf6873d8b0806c6f26ca3da60a6c45865b38ea26821975d282b4711b4c850215712cf880fa19da3a4ccee63003fd52c1074cf7080542303363323764363361376139653263383532623736616565333866353165646430376630383961353366343362303837656535373831316162663631623139386239090c660fd5ac33e738bc3482dc6fc75cd3dc5adbe41cc527c64a47db09591f80795e2713094e24aee182e86b167ac0ea398d05e33dbdad1b4026136312e214799e0301020000000000000000000005"
    |> Option.get
    |> Transaction.toExtended
let payoutTx2 =
    Transaction.fromHex "000000000101d79ab5e887493e7986755ffe09d43106bf9cb11a027cb190a22121b9347d529e0002022017ba8c88eb030b31a0da51ec5fe7bbd08ff807943f57b4cc4a33de4aca37fca6007e0000012a05f1ff01000000010002016203032229522443cf166e28468c58a4719ce01eb2d9b5b656ecae6e959001bbe8c46952bab44713303b8b08ce4bfd2203db1a929aedf9c76b46f167139c5b4cab87464d2c0fb981ea47c6eefef176db2fd26c629cfb5b7fca32bab656c91ed60c528502826700abbf8805a203197e4ad548e4eaa2b16f683c013e31d316f387ecf7adc65b3fb2065061796f7574010c02065061796f7574068010303230313334346463333433663061633664306431643564366536333838613964633439356666323330623635303536353435356630343063346162643536356331643330313830663234646233326161313838313935363634366433636362623634376466373134353564653130636639386236333538313065383837303930366135366236333830303032623637095369676e61747572650c0242303332323239353232343433636631363665323834363863353861343731396365303165623264396235623635366563616536653935393030316262653863343639098a217484c9f769d64e06abb6fc55eaf355c12c1445ccbc7ca7f35037290e4c157bbb785b2a1cc3f90527527c93644b3dfeb1bea3101ed9c616c6357310ed60594230336332376436336137613965326338353262373661656533386635316564643037663038396135336634336230383765653537383131616266363162313938623909b7c271b908f30748cd4d0d2c01d327bc340653f7d383131948f5dabec5ea231643b8d5fff8fd47a8b60d5dbcaa3a479e402c0a1dbb8d00c9063a442e535723d80301020000000000000000000005"
    |> Option.get
    |> Transaction.toExtended
let allocationTx1=
    Transaction.fromHex "000000000101dfd1a11c2ac8c5b404d2660586de78049f5f65790cd75a7f47ffd55c6ed451270002022017ba8c88eb030b31a0da51ec5fe7bbd08ff807943f57b4cc4a33de4aca37fca6007e0000012a05f1fd0100000001000201620303c27d63a7a9e2c852b76aee38f51edd07f089a53f43b087ee57811abf61b198b9f0145d90d8fe9bc32d38aacbded360bf4c7d678b7696ac7452fa40cb91b36f365c6c395e6fe445980811af284760842383de64efb4dd612a09287da5b8e929bb02816200abbf8805a203197e4ad548e4eaa2b16f683c013e31d316f387ecf7adc65b3fb20a416c6c6f636174696f6e010c02095369676e61747572650c0242303332323239353232343433636631363665323834363863353861343731396365303165623264396235623635366563616536653935393030316262653863343639091e1a89520ef501dc8a59ad93125102789486693f1563f069b8d2287f32cd53951b914edfa6d414cbbe31bc64b27d952ee42f48e5b4b33177d2bbca69df6ad62142303363323764363361376139653263383532623736616565333866353165646430376630383961353366343362303837656535373831316162663631623139386239096137f3521f298cc554bc639f54f3a461cbedcc685b2633c23ff6cf7de2630fb4011ca07e6eeeb7736c70bc167f4fcdc1a24d8b7c5a4a580f8a69a23dc5171b4d0a416c6c6f636174696f6e0604303130340301020000000000000000000005"
    |> Option.get
    |> Transaction.toExtended
let allocationTx2 =
    Transaction.fromHex "000000000101d52dcc4cef1d651035a10f75a6d0990a298becdd38649d3ba778f39b21b9620e0002022017ba8c88eb030b31a0da51ec5fe7bbd08ff807943f57b4cc4a33de4aca37fca6007e0000012a05f36501000000010002016203032229522443cf166e28468c58a4719ce01eb2d9b5b656ecae6e959001bbe8c469ddab9b41598a03fdcc5b1ed8ddbad9cef9f469cd07a4b678e8b8b1adb401579971a8e9937c74e3f2dfaf617f453d1b2bb15a1e3240b001956ce1170450a3c12102816200abbf8805a203197e4ad548e4eaa2b16f683c013e31d316f387ecf7adc65b3fb20a416c6c6f636174696f6e010c02095369676e61747572650c024230333232323935323234343363663136366532383436386335386134373139636530316562326439623562363536656361653665393539303031626265386334363909a0a7f58191dfaccf7e68a5bfbcc241b48cb73921e3611dc42c5bb25784b956ef54317935adb7430f5ac745961e748cc3e5825711baaeeec47e74994ec58e80d5423033633237643633613761396532633835326237366165653338663531656464303766303839613533663433623038376565353738313161626636316231393862390995e18de0085e04dc84eb61aa17dfcffa578c2ba7bb4d75c6f12d6a32f537c0c73883400b57c0834a146804a7abef80765cfb7315f6baa659517771d50a1e167b0a416c6c6f636174696f6e0604303130630301020000000000000000000005"
    |> Option.get
    |> Transaction.toExtended

let hashPayout = "44a69664aa2096a51e093f7ec7fb7840c570cd7ba1f86f1e67dbf0b1d34b2f17"

let generateGetUTXO utxos pt =
    match Map.tryFind pt utxos with
    | Some op -> op
    | None -> UtxoSet.NoOutput

let contractPath = NUnit.Framework.TestContext.CurrentContext.TestDirectory
    //Path.Combine [| Path.GetTempPath(); Path.GetRandomFileName() |]

let compile code = lazy (result {
    let! hints = Contract.recordHints code
    let! queries = Infrastructure.ZFStar.totalQueries hints

    let contract = {
        code = code
        hints = hints
        rlimit = rlimit
        queries = queries
    }

    return!
        Contract.compile contractPath contract
        |> Result.bind (Contract.load contractPath 100ul code)
})

let compiledVotingContract =
    lazy (Contract.load contractPath 100ul VotingContractCode.contractCode chainParams.votingContractId)
    //compile VotingContractCode.contractCode

let executeVotingContract (txSkeleton : TxSkeleton.T) context messageBody =
    result {
        let! contract = compiledVotingContract.Force()
        
        let command = "Payout"
        
        let! (txSkel,_,_) = Contract.run contract txSkeleton context command Zen.Types.Main.sender.Anonymous messageBody [] None
        
        let cw = {
            contractId      = votingContractId
            command         = command
            messageBody     = messageBody
            stateCommitment = NoState
            beginInputs     = txSkeleton.pInputs |> List.length |> uint32
            beginOutputs    = txSkeleton.outputs |> List.length |> uint32
            inputsLength    = List.length txSkel.pInputs - List.length txSkeleton.pInputs |> uint32
            outputsLength   = List.length txSkel.outputs - List.length txSkeleton.outputs |> uint32
            signature       = None
            cost            = 5UL
        }
        
        let inputs =
            txSkel.pInputs
            |> List.map (function TxSkeleton.PointedOutput (pt,op) -> Outpoint pt | TxSkeleton.Mint s -> Mint s)
        
        let utxosInputs =
            txSkel.pInputs
            |> List.choose ((function TxSkeleton.PointedOutput (pt,op) -> Some (pt,op) | TxSkeleton.Mint s -> None))
            |> List.fold (fun m (pt,op) -> Map.add pt (UtxoSet.Unspent op) m) Map.empty
        
        let tx =
            {
                version   = 0u
                inputs    = inputs
                outputs   = txSkel.outputs
                witnesses = [ContractWitness cw]
                contract  = None
            }
        
        let ex = Transaction.toExtended tx
        
        return (ex, txSkel)
    }

let context0 = { blockNumber=0u; timestamp=1UL }

let createMessageBody (command : byte[]) (serBallot : byte[]) (interval : uint32) (keys : List<Crypto.KeyPair>) : ZData.data option =
    
    let serialize mkZData = mkZData >> Serialization.Data.serialize >> FsBech32.Base16.encode
    
    let ballotId = serBallot |> FsBech32.Base16.encode |> ZFStar.fsToFstString
    
    let serBallot   = serialize ZData.String ballotId 
    let serInterval = serialize ZData.U32    interval
    
    let digest = Hash.compute (String.concat "" [serInterval; serBallot] |> System.Text.Encoding.Default.GetBytes)
    
    let signatures =
        keys
        |> List.map (fun (sk, pk) -> (pk, Crypto.sign sk digest))
        |> List.map (fun (pk, sign) -> (pk |> CryptoPublicKey.toString |> ZFStar.fsToFstString, sign |> ZFStar.fsToFstSignature |> ZData.Signature))
        |> Map.ofList
        
    let msgBody =
        Map.empty
        |> Map.add command (ballotId |> ZData.String)
        |> Map.add "Signature"B ((signatures, signatures |> Map.toList |> List.length |> uint32) |> ZData.Dict |> ZData.Collection)
    
    (msgBody, msgBody |> Map.toList |> List.length |> uint32) |> ZData.Dict |> ZData.Collection |> Some

let createValidMessageBody (ballot : Ballot) (interval : uint32) (keys : List<Crypto.KeyPair>) : ZData.data option =
    
    let command = match ballot with | Allocation _ -> "Allocation"B | Payout _ -> "Payout"B
    
    let serBallot = Ballot.serialize ballot
    
    createMessageBody command serBallot interval keys

let executeValidVotingContract (ballot : Ballot) (interval : uint32) (keys : List<Crypto.KeyPair>) =
    let context = { context0 with blockNumber = interval * chainParams.intervalLength + chainParams.snapshot + 1u }
    executeVotingContract TxSkeleton.empty context (createValidMessageBody ballot interval keys)

let executeVotingContractInContext (ballot : Ballot) (interval : uint32) (keys : List<Crypto.KeyPair>) context =
    executeVotingContract TxSkeleton.empty context (createValidMessageBody ballot interval keys)

let executeVotingContractWithMessageBody (msgBody : ZData.data option) (interval : uint32) =
    let context = { context0 with blockNumber = interval * chainParams.intervalLength + chainParams.snapshot + 1u }
    executeVotingContract TxSkeleton.empty context msgBody

let ema : EMA.T =
    {
        difficulty = difficulty
        delayed    = []
    }

[<Test>]
let ``add block in repository `` () =
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = 0u; blockHash = Hash.zero}
    |> DataAccess.Tip.put dataAccess session
    
    let block:Block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = 1u
                    difficulty  = difficulty
                    commitments = Hash.zero
                    timestamp   = 1UL
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = Hash.zero
            commitments                 = []
            transactions                = [payoutTx1;allocationTx1;allocationTx2]
        }
    let blockHash = Block.hash block.header

    Repository.addBlock dataAccess session chainParams blockHash block
    let interval = CGP.getInterval chainParams block.header.blockNumber
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.defaultWith ( fun() -> failwith "No PK Allocation map found" )
    |> ignore

[<Test>]
let ``a vote doesn't register if the VotingContract doesn't have neither of the "Payout" or "Allocation" command.`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Payout (ContractRecipient votingContractId, [])
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody "Banana"B serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore

[<Test>]
let ``a vote registers if the VotingContract has the "Payout" command.`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Payout (ContractRecipient votingContractId, [])
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody "Payout"B serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.defaultWith ( fun _ -> failwith "valid vote wasn't registered" )
    |> ignore

[<Test>]
let ``a vote registers if the VotingContract has the "Allocation" command.`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Ballot.Allocation 13uy
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody "Allocation"B serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.defaultWith ( fun _ -> failwithf "[%A]" msgBody ) //"valid vote wasn't registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore

[<Test>]
let ``voting Payout with Allocation body`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Ballot.Allocation 13uy
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody "Payout"B serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid Allocation vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid Payout vote was registered" )
    |> ignore

[<Test>]
let ``voting Allocation with Payout body`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Payout (ContractRecipient votingContractId, [])
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody "Allocation"B serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid Allocation vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid Payout vote was registered" )
    |> ignore

let ``make sure the tally updates when it should`` () =
    
    failwith "DAVID NEEDS TO IMPLEMENT IT"
    
    let context = { context0 with blockNumber = 105ul } // Testnet tally block 
    
    let tempDir () =
        System.IO.Path.Combine
            [| System.IO.Path.GetTempPath(); System.IO.Path.GetRandomFileName() |]
    let dataPath = tempDir()
    
    let createBroker () =
         FsNetMQ.Actor.create (fun shim ->
            use poller = FsNetMQ.Poller.create ()
            use emObserver = FsNetMQ.Poller.registerEndMessage poller shim
    
            use sbBroker = ServiceBus.Broker.create poller "test" None
            use evBroker = EventBus.Broker.create poller "test" None
    
            FsNetMQ.Actor.signal shim
            FsNetMQ.Poller.run poller
        )
    
    let getActors () =
        [
            createBroker ()
            Blockchain.Main.main dataPath chainParams "test" false
            Tally.Main.main dataPath "test" Chain.Test Tally.Main.NoWipe
        ]
        |> List.rev
        |> List.map Disposables.toDisposable
        |> Disposables.fromList
    
    let databaseContext = Blockchain.DatabaseContext.createEmpty dataPath
    use session = Blockchain.DatabaseContext.createSession databaseContext

    let client = ServiceBus.Client.create "test"
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = []
        }
    
    let exHeader = Blockchain.ExtendedBlockHeader.create Blockchain.ExtendedBlockHeader.MainChain Blockchain.ExtendedBlockHeader.empty (Block.hash block.header) block
    
    //Messaging.Services.Blockchain.bl
    Blockchain.BlockHandler.addBlocks session exHeader Blockchain.ExtendedBlockHeader.empty
    |> ignore
