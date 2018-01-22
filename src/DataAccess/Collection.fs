[<CompilationRepresentation (CompilationRepresentationFlags.ModuleSuffix)>]
module DataAccess.Collection

open System
open Lmdb
open System.Runtime.InteropServices

let bufferSize = 1024 * 1024 * 1024 // One-megabyte
let structSize = IntPtr.Size * 2 |> IntPtr

let create (session:Session) name keySerializer valueSerializer valueDeseralizer =
    let mutable db = 0ul
        
    mdb_dbi_open(session.tx, name, MDB_CREATE, &db)
    |> checkErrorCode 

    {
        environment=session.env
        database=db;        
        keySerializer=keySerializer
        valueSerializer=valueSerializer
        valueDeseralizer=valueDeseralizer
        indices=[]
    }
                     
let addIndex index collection =
    let indexFunc (session:Session) key value = 
        let key,value = index.getIndexKeys session key value 
        let key = index.indexKeySerializer key
        let value = collection.keySerializer value
                   
        use pinnedKey = pin key
        use pinnedValue = pin value                   
                                                     
        let mutable keyData = byteArrayToData pinnedKey
        let mutable valueData = byteArrayToData pinnedValue               
                        
        let result = 
            mdb_put(session.tx, index.database, &keyData, &valueData, MDB_NODUPDATA)

        if result <> 0 && result <> MDB_KEYEXIST then
            errorToString result 
            |> failwith             
 
    let indices = indexFunc :: collection.indices
    {collection with indices=indices}
            
let tryGet collection (session:Session) key =
    let keyBytes = collection.keySerializer key 
    use pinnedKey = pin keyBytes

    let mutable keyData = byteArrayToData pinnedKey
    let mutable valueData = Data.empty
     
    let result = mdb_get (session.tx,collection.database,&keyData,&valueData)

    if result = 0 then    
        dataToByteArray valueData
        |> collection.valueDeseralizer
        |> Some
    elif result = MDB_NOTFOUND then
        None
    else
        errorToString result |> failwith     
        
let get collection (session:Session) key = 
    match tryGet collection session key with
    | Some value -> value
    | None -> failwith "key was not found in the collection"  
        
let put collection session key value =
    let keyBytes = collection.keySerializer key
    let valueBytes = collection.valueSerializer value
    
    use pinnedKey = pin keyBytes
    use pinnedValue = pin valueBytes      
    
    let mutable keyData = byteArrayToData pinnedKey
    let mutable valueData = byteArrayToData pinnedValue
    mdb_put(session.tx, collection.database, &keyData, &valueData, 0ul)
    |> checkErrorCode
    
    List.iter (fun indexFunc -> indexFunc session key value) collection.indices         
    
let containsKey collection (session:Session) key = 
    match tryGet collection session key with
    | Some _ -> true
    | None -> false     