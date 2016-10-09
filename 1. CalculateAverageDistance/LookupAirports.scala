//Filter Function
def notHeader(row: String): Boolean = {
    !row.contains("Description")
}

//Parse Lookup Function
def parseLookup(row: String): (String,String)={
 val x = row.replace("\"","").split(',')
 (x(0),x(1))
}

//1. Load text file for airports data 
//2. Filter using notheader function and 
//3. Map using parseLookup function
val airports=sc.textFile(airportsPath).filter(notHeader).map(parseLookup)

airports.lookup("PPG")

val airportLookup=airports.collectAsMap

//pass in the keys to lookup values from dictionary
airportLookup("CLI")

airportAvgDelay.map(x=>(airportLookup(x._1),x._2)).take(10)

//use broadcast action to cache it in memory
val airportBC=sc.broadcast(airportLookup)

//use broadcast.valu() function to lookup values
airportAvgDelay.map(x => (airportBC.value(x._1),x._2)).sortBy(-_._2).take(10)