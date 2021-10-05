#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 


open System
open System.Diagnostics
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic

type Gossip =
    |SetNbrs of IActorRef[] 
    |BeginGossip of String
    |ConvMsgGossip of String
    |SetValues of int * IActorRef[] * int
    |PushSum of float * float
    |BeginPushSum of int
    |ConvPushSum of float * float
    |PrintNarray of int

let r  = System.Random()

let timer = Stopwatch()

let system = ActorSystem.Create("System")

let mutable conTime=0


let Listener (mailbox:Actor<_>) = 

    let mutable noOfMsg = 0
    let mutable noOfNde = 0
    let mutable startTime = 0
    let mutable totNodes =0
    let mutable allNds:IActorRef[] = [||]

    let rec loop() = actor {

            let! message = mailbox.Receive()
            match message with 

            | ConvMsgGossip message ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                noOfMsg <- noOfMsg + 1

                if noOfMsg = totNodes then
                    let rTime = timer.ElapsedMilliseconds
                    printfn "Time for Convergence from timer: %A ms" rTime
                    printfn "Time for Convergence from System Time: %A ms" (endTime-startTime)
                    conTime <-endTime-startTime
                    Environment.Exit 0

                else
                    let newStart= r.Next(0,allNds.Length)
                    allNds.[newStart] <! BeginGossip("Hello")

            | ConvPushSum (s,w) ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                noOfNde <- noOfNde + 1

                if noOfNde = totNodes then
                    let rTime = timer.ElapsedMilliseconds
                    printfn "Time for Convergence from timer: %A ms" rTime
                    printfn "Time for Convergence from System Time: %A ms" (endTime-startTime)
                    conTime <-endTime-startTime
                    Environment.Exit 0

                else
                    let newStart=r.Next(0,allNds.Length)
                    allNds.[newStart] <! PushSum(s,w)
          
            | SetValues (strtTime,nodesRef,totNds) ->
                startTime <-strtTime
                allNds <- nodesRef
                totNodes <-totNds
                
            | _->()

            return! loop()
        }
    loop()

    

let Node listener nodeNum (mailbox:Actor<_>)  =

    let mutable numMsgHeard = 0 
    let mutable nbrs:IActorRef[]=[||]

    let mutable sum1= nodeNum |> float
    let mutable weight = 1.0
    let mutable termRound = 1.0
    let mutable flag = 0
    let mutable counter = 1
    let mutable ratio1 = 0.0
    let mutable ratio2 = 0.0
    let mutable ratio3 = 0.0
    let mutable ratio4 = 0.0
    let mutable convflag = 0
    let ratiolimit = 10.0**(-10.0)
    
    let rec loop() = actor {

        let! message = mailbox.Receive()
        match message with 

        |BeginPushSum ind->
            let index = r.Next(0,nbrs.Length)
            let sn = index |> float
            nbrs.[index] <! PushSum(sn,1.0)

        |PushSum (s,w)->
            if(convflag = 1) then
                let index = r.Next(0,nbrs.Length)
                nbrs.[index] <! PushSum(s,w)
            
            if(flag = 0) then
                if(counter = 1) then
                    ratio1<-sum1/weight
                else if(counter = 2) then
                    ratio2<-sum1/weight
                else if(counter = 3) then
                    ratio3<-sum1/weight
                    flag<-1

                counter<-counter+1

            sum1<-sum1+s
            sum1<-sum1/2.0
            weight<-weight+w
            weight<-weight/2.0
            
            ratio4<-sum1/weight
           
            if(flag=0) then
                let index= r.Next(0,nbrs.Length)
                nbrs.[index] <! PushSum(sum1,weight)                

            
            if(abs(ratio1-ratio4)<=ratiolimit && convflag=0) then 
                      convflag<-1
                      listener <! ConvPushSum(sum1,weight)

            else
                      ratio1<-ratio2
                      ratio2<-ratio3
                      ratio3<-ratio4
                      let index= r.Next(0,nbrs.Length)
                      nbrs.[index] <! PushSum(sum1,weight)

        | SetNbrs nArray->
                nbrs<-nArray
        | PrintNarray num->
                printfn "%A" nbrs


        | BeginGossip msg ->

                numMsgHeard<- numMsgHeard+1

                if(numMsgHeard = 10) then
                      listener <! ConvMsgGossip(msg)

                else
                      let index= r.Next(0,nbrs.Length)
                      nbrs.[index] <! BeginGossip(msg)
        | _-> ()

        return! loop()
    }
    loop()

module prtcl=

        let callPrtcl algo numN nodeArr=
            (nodeArr : _ array)|>ignore

            if algo="gossip" then 
                let starter= r.Next(0,numN-1)
                nodeArr.[starter]<!BeginGossip("Hello")

            elif algo="push-sum" then
                let starter= r.Next(0,numN-1)
                nodeArr.[starter]<!BeginPushSum(starter)

            else
                printfn"Wrong Argument!"

            

module tplgy=
               
        let buildFull numN algo=
            let listener= 
                Listener
                    |> spawn system "listener"

            let nodeArray = Array.zeroCreate(numN)

            let mutable nbrArray:IActorRef[]=Array.empty
            
            for i in [0..numN-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))
            
            for i in [0..numN-1] do
                if i=0 then
                    nbrArray<-nodeArray.[1..numN-1]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=(numN-1) then 
                    nbrArray<-nodeArray.[0..(numN-2)]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                else
                    nbrArray<-Array.append nodeArray.[0..i-1] nodeArray.[i+1..numN-1]
                    nodeArray.[i]<!SetNbrs(nbrArray)
           
            timer.Start()
                   
            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numN)

            prtcl.callPrtcl algo numN nodeArray


        let build2D numN algo=
            let listener= 
                Listener
                    |> spawn system "listener"
            
            let rCount=int(ceil ((float numN)** 0.33))
            let numNMod=rCount*rCount
            let numNM=int(numNMod)

            let nodeArray = Array.zeroCreate(numNM*rCount)
            
            for i in [0..int(numNM*rCount)-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))

            let mutable cycle = ((((numN |> float) ** 0.33) |> ceil) ** 2.0) |> int
            let mutable step = 0

            // printfn "%A" nodeArray
            printfn "%d %d %d" cycle rCount step

            for layer in [1..rCount] do
                for i in [0..numNM-1] do
                    // printfn "%d %d" layer (step+i) 
                    let mutable nbrArray:IActorRef[]=Array.empty
                    if i=0 then
                        nbrArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i+rCount..step+i+rCount]

                    elif i=rCount-1 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+rCount..step+i+rCount]
                    
                    elif i=numNM-rCount then 
                        nbrArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i-rCount..step+i-rCount]

                    elif i=numNM-1 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i-rCount..step+i-rCount]

                    elif i<rCount-1 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i+rCount..step+i+rCount] 

                    elif i>numNM-rCount && i<numNM-1 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i-rCount..step+i-rCount] 

                    elif i%rCount = 0 then 
                        nbrArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i-rCount..step+i-rCount] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i+rCount..step+i+rCount] 

                    elif (i+1)%rCount = 0 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i-rCount..step+i-rCount] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i+rCount..step+i+rCount] 
                   
                    else
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i-rCount..step+i-rCount] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i+rCount..step+i+rCount] 

                    if layer =0 then
                        nbrArray <- Array.append nbrArray nodeArray.[i+numNM..i+numNM]
                    elif layer=cycle-1 then
                        nbrArray <- Array.append nbrArray nodeArray.[step+i-numNM..step+i-numNM]
                    else
                        nbrArray <- Array.append nbrArray nodeArray.[step+i-numNM..step+i-numNM]
                        nbrArray <- Array.append nbrArray nodeArray.[step+i+numNM..step+i+numNM]

                    nodeArray.[step+i]<!SetNbrs(nbrArray)

                step <- step+cycle

            timer.Start()

            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numNM)

            // // printfn "%A" nodeArray
            // (nodeArray.[0] <! PrintNarray(1))
            // (nodeArray.[1] <! PrintNarray(1))
            // (nodeArray.[17] <! PrintNarray(1))

            prtcl.callPrtcl algo numN nodeArray



        let buildLine numN algo=

            let listener= 
                Listener
                    |> spawn system "listener"

            let nodeArray = Array.zeroCreate(numN)
            
            for i in [0..numN-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))
            
            let mutable nbrArray:IActorRef[]=Array.empty


            for i in [0..numN-1] do
                if i=0 then
                    nbrArray<-nodeArray.[1..1]    
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=(numN-1) then 
                    nbrArray<-nodeArray.[(numN-2)..(numN-2)]                   
                    nodeArray.[i]<!SetNbrs(nbrArray)

                else
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1]            
                    nodeArray.[i]<!SetNbrs(nbrArray)
            
            timer.Start()

            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numN)

            prtcl.callPrtcl algo numN nodeArray
        

        let buildImp2D numN algo=

            let listener= 
                Listener
                    |> spawn system "listener"
            
            let rCount=int(ceil ((float numN)** 0.33))
            let numNMod=rCount*rCount
            let numNM=int(numNMod)

            let nodeArray = Array.zeroCreate(numNM*rCount)
            
            for i in [0..int(numNM*rCount)-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))

            let mutable cycle = ((((numN |> float) ** 0.33) |> ceil) ** 2.0) |> int
            let mutable step = 0


            // printfn "%A" nodeArray
            printfn "%d %d %d" cycle rCount step

            for layer in [1..rCount] do
                for i in [0..numNM-1] do
                    // printfn "%d %d" layer (step+i) 
                    let mutable nbrArray:IActorRef[]=Array.empty
                    if i=0 then
                        nbrArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i+rCount..step+i+rCount]

                    elif i=rCount-1 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+rCount..step+i+rCount]

                    elif i=numNM-rCount then 
                        nbrArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i-rCount..step+i-rCount]

                    elif i=numNM-1 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i-rCount..step+i-rCount]

                    elif i<rCount-1 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i+rCount..step+i+rCount] 
                        
                   

                    elif i>numNM-rCount && i<numNM-1 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i-rCount..step+i-rCount] 

                    elif i%rCount = 0 then 
                        nbrArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i-rCount..step+i-rCount] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i+rCount..step+i+rCount] 

                    elif (i+1)%rCount = 0 then 
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i-rCount..step+i-rCount] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i+rCount..step+i+rCount] 
                   
                    else
                        nbrArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i-rCount..step+i-rCount] 
                        nbrArray<-Array.append nbrArray nodeArray.[step+i+rCount..step+i+rCount] 

                    if layer =0 then
                        nbrArray <- Array.append nbrArray nodeArray.[i+numNM..i+numNM]
                        nbrArray <- Array.append nbrArray nodeArray.[i+numNM+1..i+numNM+1]

                    elif layer=cycle-1 then
                        nbrArray <- Array.append nbrArray nodeArray.[step+i-numNM..step+i-numNM]
                        nbrArray <- Array.append nbrArray nodeArray.[step+i-numNM-1..step+i-numNM-1]

                    else
                        nbrArray <- Array.append nbrArray nodeArray.[step+i-numNM..step+i-numNM]
                        nbrArray <- Array.append nbrArray nodeArray.[step+i+numNM..step+i+numNM]
                        nbrArray <- Array.append nbrArray nodeArray.[step+i+numNM-1..step+i+numNM-1]


                    
                    nodeArray.[step+i]<!SetNbrs(nbrArray)

                step <- step+cycle

            timer.Start()

            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numNM)

            // // printfn "%A" nodeArray
            // (nodeArray.[0] <! PrintNarray(1))
            // (nodeArray.[1] <! PrintNarray(1))
            (nodeArray.[1] <! PrintNarray(1))

            prtcl.callPrtcl algo numN nodeArray



        let createTopology top numN algo=
           if top="full" then buildFull numN algo
           elif top="2D" then build2D numN algo
           elif top="line" then buildLine numN algo
           elif top="imp2D" then buildImp2D numN algo
           else printfn"Wrong Argument!"
             
module mainModule=

        let args : string array = fsi.CommandLineArgs |> Array.tail
        let mutable numNodes=args.[0] |> int
        let topology=args.[1] |> string
        let algo=args.[2] |>string
        
        printfn "%s %s" topology algo

        tplgy.createTopology topology numNodes algo
   
        System.Console.ReadLine() |> ignore