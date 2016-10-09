//Program - Calculate the average delay per airport on flight dataset (Flights CSV)

sc

// Data location
val airlinesPath="hdfs:////user/bhaskarbabu09846098/sparkdata/airlines.csv"
val airportsPath="hdfs:///user/bhaskarbabu09846098/sparkdata/airports.csv"
val flightsPath="hdfs:///user/bhaskarbabu09846098/sparkdata/flights.csv"


import org.joda.time._
import org.joda.time.format._
import org.joda.time.LocalTime
import org.joda.time.LocalDate

case class Flight(date: LocalDate,
                  airline: String ,
                  flightnum: String,
                  origin: String ,
                  dest: String ,
                  dep: LocalTime,
                  dep_delay: Double,
                  arv: LocalTime,
                  arv_delay: Double ,
                  airtime: Double ,
                  distance: Double
                   )

def parse(row: String): Flight={

  val fields = row.split(",")
  val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
  val timePattern = DateTimeFormat.forPattern("HHmm")

  val date: LocalDate = datePattern.parseDateTime(fields(0)).toLocalDate()
  val airline: String = fields(1)
  val flightnum: String = fields(2)
  val origin: String = fields(3)
  val dest: String = fields(4)
  val dep: LocalTime = timePattern.parseDateTime(fields(5)).toLocalTime()
  val dep_delay: Double = fields(6).toDouble
  val arv: LocalTime = timePattern.parseDateTime(fields(7)).toLocalTime()
  val arv_delay: Double = fields(8).toDouble
  val airtime: Double = fields(9).toDouble
  val distance: Double = fields(10).toDouble
  
  Flight(date,airline,flightnum,origin,dest,dep,
         dep_delay,arv,arv_delay,airtime,distance)
}

//Load flights.csv as RDD 
val flights=sc.textFile(flightsPath)

flights

// The total number of records 
flights.count()

// The first row
flights.first()

flights.map(_.split(","))

flights.map(x => x.split(","))

//Parse the text data to FLight Object using 'parse' function
val flightsParsed=flights.map(parse)

// Let's take a look at the data in the Parsed RDD 
flightsParsed.first()

//Create RDD pair RDD(key --> origin, value --> delay)
val airportDelays = flightsParsed.map(x => (x.origin,x.dep_delay))

airportDelays.keys.take(10)

airportDelays.values.take(10)

//calculate total delay by key using reduceByKey function
val airportTotalDelay=airportDelays.reduceByKey((x,y) => x+y)

//map count 1 and count total number of delays by key
val airportCount=airportDelays.mapValues(x => 1).reduceByKey((x,y) => x+y)

//Join Total Delay and Count RDDs
val airportSumCount=airportTotalDelay.join(airportCount)

//Calculate average delay per airport
val airportAvgDelay=airportSumCount.mapValues(x => x._1/x._2.toDouble)

//Top 10 airports based on delay
airportAvgDelay.sortBy(-_._2).take(10)

//Calculate average delay using combineByKey
val airportSumCount2=airportDelays.combineByKey(
                                            value => (value,1),
                                            (acc: (Double,Int), value) =>  (acc._1 + value, acc._2+1),
                                            (acc1: (Double,Int), acc2: (Double,Int)) => (acc1._1+acc2._1,acc1._2+acc2._2))

val airportAvgDelay2=airportSumCount2.mapValues(x => x._1/x._2.toDouble)

//Top 10 airports based on delay
airportAvgDelay2.sortBy(-_._2).take(10)